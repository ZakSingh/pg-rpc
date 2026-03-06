/// Automatic grouping of double-underscore prefixed columns into nested structs.
///
/// Columns named `{prefix}__{field}` are grouped into a nested struct named after
/// the prefix. For example, `product__name`, `product__slug`, `product__msrp`
/// become a `Product` struct with fields `name`, `slug`, `msrp`.

/// Represents a field that may be a plain column or a group of columns
/// sharing a double-underscore prefix.
#[derive(Debug)]
pub enum GroupedField<T> {
    /// A regular column, passed through unchanged.
    Plain(T),
    /// A group of columns sharing a double-underscore prefix.
    Group {
        /// The prefix (e.g., "product" from "product__name").
        group_name: String,
        /// The fields within the group, with the prefix stripped from their names.
        fields: Vec<T>,
        /// True if the group should be wrapped in `Option<Struct>`.
        /// This is set when ALL fields in the group have `nullable_due_to_join`.
        optional: bool,
    },
}

/// Groups items whose names contain `__` by their prefix (the part before the first `__`).
///
/// - Splits on the **first** `__` only (`a__b__c` → group `a`, field name `b__c`).
/// - Groups appear at the position of their first member column.
/// - A single column with `__` still forms a group.
/// - Items without `__` pass through as `Plain`.
///
/// # Arguments
///
/// * `items` - The items to group.
/// * `get_name` - Accessor to get the name of an item.
/// * `is_join_nullable` - Returns true if the item is nullable due to a join.
/// * `set_name` - Mutator to set the name of an item (used to strip the prefix).
pub fn group_by_double_underscore<T>(
    items: Vec<T>,
    get_name: impl Fn(&T) -> &str,
    is_join_nullable: impl Fn(&T) -> bool,
    mut set_name: impl FnMut(&mut T, String),
) -> Vec<GroupedField<T>> {
    use indexmap::IndexMap;

    // First pass: identify groups and their positions.
    // Using IndexMap to preserve insertion order (= position of first member).
    let mut groups: IndexMap<String, Vec<(usize, T)>> = IndexMap::new();
    let mut plains: Vec<(usize, T)> = Vec::new();

    for (i, item) in items.into_iter().enumerate() {
        let name = get_name(&item).to_string();
        if let Some((prefix, _suffix)) = name.split_once("__") {
            groups
                .entry(prefix.to_string())
                .or_default()
                .push((i, item));
        } else {
            plains.push((i, item));
        }
    }

    // Second pass: build the result, ordered by first appearance.
    // We need to interleave groups and plains by their original position.
    let mut result_entries: Vec<(usize, GroupedField<T>)> = Vec::new();

    for (group_name, members) in groups {
        let first_pos = members[0].0;
        let optional = members.iter().all(|(_, item)| is_join_nullable(item));

        let fields: Vec<T> = members
            .into_iter()
            .map(|(_, mut item)| {
                let name = get_name(&item).to_string();
                // Strip the prefix and separator
                let suffix = name.split_once("__").unwrap().1.to_string();
                set_name(&mut item, suffix);
                item
            })
            .collect();

        result_entries.push((
            first_pos,
            GroupedField::Group {
                group_name,
                fields,
                optional,
            },
        ));
    }

    for (pos, item) in plains {
        result_entries.push((pos, GroupedField::Plain(item)));
    }

    // Sort by original position to preserve field ordering.
    result_entries.sort_by_key(|(pos, _)| *pos);

    result_entries.into_iter().map(|(_, gf)| gf).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestCol {
        name: String,
        join_nullable: bool,
    }

    impl TestCol {
        fn new(name: &str, join_nullable: bool) -> Self {
            Self {
                name: name.to_string(),
                join_nullable,
            }
        }
    }

    fn do_group(items: Vec<TestCol>) -> Vec<GroupedField<TestCol>> {
        group_by_double_underscore(
            items,
            |c| &c.name,
            |c| c.join_nullable,
            |c, new_name| c.name = new_name,
        )
    }

    #[test]
    fn test_no_double_underscore() {
        let cols = vec![
            TestCol::new("id", false),
            TestCol::new("name", false),
        ];
        let result = do_group(cols);
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0], GroupedField::Plain(c) if c.name == "id"));
        assert!(matches!(&result[1], GroupedField::Plain(c) if c.name == "name"));
    }

    #[test]
    fn test_basic_grouping() {
        let cols = vec![
            TestCol::new("id", false),
            TestCol::new("product__name", false),
            TestCol::new("product__slug", false),
            TestCol::new("title", false),
        ];
        let result = do_group(cols);
        assert_eq!(result.len(), 3); // id, product group, title

        assert!(matches!(&result[0], GroupedField::Plain(c) if c.name == "id"));
        match &result[1] {
            GroupedField::Group {
                group_name,
                fields,
                optional,
            } => {
                assert_eq!(group_name, "product");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "name");
                assert_eq!(fields[1].name, "slug");
                assert!(!optional);
            }
            _ => panic!("expected Group"),
        }
        assert!(matches!(&result[2], GroupedField::Plain(c) if c.name == "title"));
    }

    #[test]
    fn test_single_column_group() {
        let cols = vec![
            TestCol::new("id", false),
            TestCol::new("owner__name", false),
        ];
        let result = do_group(cols);
        assert_eq!(result.len(), 2);

        match &result[1] {
            GroupedField::Group {
                group_name,
                fields,
                ..
            } => {
                assert_eq!(group_name, "owner");
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "name");
            }
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn test_optional_group_from_join() {
        let cols = vec![
            TestCol::new("id", false),
            TestCol::new("product__name", true),
            TestCol::new("product__slug", true),
        ];
        let result = do_group(cols);
        assert_eq!(result.len(), 2);

        match &result[1] {
            GroupedField::Group { optional, .. } => {
                assert!(optional, "group should be optional when fields are join-nullable");
            }
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn test_required_if_not_all_fields_join_nullable() {
        let cols = vec![
            TestCol::new("product__name", false),
            TestCol::new("product__slug", true), // only one is join-nullable
        ];
        let result = do_group(cols);

        match &result[0] {
            GroupedField::Group { optional, .. } => {
                assert!(!optional, "group should be required if not ALL fields are join-nullable");
            }
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn test_multiple_groups() {
        let cols = vec![
            TestCol::new("id", false),
            TestCol::new("product__name", false),
            TestCol::new("product__slug", false),
            TestCol::new("owner__display_name", true),
            TestCol::new("owner__id", true),
            TestCol::new("title", false),
        ];
        let result = do_group(cols);
        assert_eq!(result.len(), 4); // id, product, owner, title

        assert!(matches!(&result[0], GroupedField::Plain(c) if c.name == "id"));

        match &result[1] {
            GroupedField::Group {
                group_name,
                optional,
                ..
            } => {
                assert_eq!(group_name, "product");
                assert!(!optional);
            }
            _ => panic!("expected product Group"),
        }

        match &result[2] {
            GroupedField::Group {
                group_name,
                optional,
                ..
            } => {
                assert_eq!(group_name, "owner");
                assert!(optional);
            }
            _ => panic!("expected owner Group"),
        }

        assert!(matches!(&result[3], GroupedField::Plain(c) if c.name == "title"));
    }

    #[test]
    fn test_split_on_first_double_underscore_only() {
        let cols = vec![TestCol::new("a__b__c", false)];
        let result = do_group(cols);

        match &result[0] {
            GroupedField::Group {
                group_name,
                fields,
                ..
            } => {
                assert_eq!(group_name, "a");
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "b__c");
            }
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn test_ordering_preserved() {
        // Groups appear at position of their first member
        let cols = vec![
            TestCol::new("alpha", false),
            TestCol::new("z__one", false),
            TestCol::new("beta", false),
            TestCol::new("z__two", false),
            TestCol::new("gamma", false),
        ];
        let result = do_group(cols);
        // Should be: alpha, z_group, beta, gamma
        // (z group appears at position of z__one, which is before beta)
        assert_eq!(result.len(), 4);
        assert!(matches!(&result[0], GroupedField::Plain(c) if c.name == "alpha"));
        assert!(matches!(&result[1], GroupedField::Group { group_name, .. } if group_name == "z"));
        assert!(matches!(&result[2], GroupedField::Plain(c) if c.name == "beta"));
        assert!(matches!(&result[3], GroupedField::Plain(c) if c.name == "gamma"));
    }
}
