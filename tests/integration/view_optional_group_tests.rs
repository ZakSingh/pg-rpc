//! A view's `prefix__column` groups become nested structs; a group that comes
//! from the nullable side of an outer join is `Option<Struct>`. Whether such
//! a group is present is decided by probing its first column, whatever that
//! column's type.

use super::*;
use crate::integration::run_helpers::{generate_project, run_binary};

#[test]
fn optional_view_group_with_non_json_first_column() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema vg;
            create table vg.owner (id int primary key, name text not null, joined date not null);
            create table vg.item (
                id int primary key,
                title text not null,
                owner_id int references vg.owner (id)
            );
            insert into vg.owner values (10, 'ann', '2024-05-06');
            insert into vg.item values (1, 'with owner', 10), (2, 'orphan', null);

            -- One group led by a text column, one by an int, one by a date.
            create view vg.item_view as
                select i.id,
                       i.title,
                       o.name   as owner__name,
                       o.id     as owner__id,
                       o2.id    as keeper__id,
                       o2.name  as keeper__name,
                       o3.joined as since__joined
                from vg.item i
                         left join vg.owner o on o.id = i.owner_id
                         left join vg.owner o2 on o2.id = i.owner_id
                         left join vg.owner o3 on o3.id = i.owner_id;

            create function vg.items() returns setof vg.item_view
                language sql as $$ select * from vg.item_view order by id $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("vg"));
        run_binary(
            &project,
            conn_string,
            "view_groups",
            "",
            r#"
    let items = vg::items().exec(&client).await.expect("rows of the view");
    assert_eq!(items.len(), 2);

    let owner = items[0].owner.as_ref().expect("item 1 has an owner");
    assert_eq!((owner.name.as_deref(), owner.id), (Some("ann"), Some(10)));
    let keeper = items[0].keeper.as_ref().expect("item 1 has a keeper");
    assert_eq!((keeper.id, keeper.name.as_deref()), (Some(10), Some("ann")));
    assert!(items[0].since.is_some());

    assert!(items[1].owner.is_none());
    assert!(items[1].keeper.is_none());
    assert!(items[1].since.is_none());
"#,
        );
    });
}
