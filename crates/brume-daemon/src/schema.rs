// @generated automatically by Diesel CLI.

diesel::table! {
    filesystems (id) {
        id -> Integer,
        uuid -> Binary,
        creation_info -> Binary,
        root_node -> Integer,
    }
}

diesel::table! {
    nodes (id) {
        id -> Integer,
        name -> Text,
        kind -> Text,
        state -> Nullable<Binary>,
        size -> Nullable<BigInt>,
        parent -> Nullable<Integer>,
        last_modified -> Nullable<Timestamp>,
    }
}

diesel::table! {
    synchros (id) {
        id -> Integer,
        uuid -> Binary,
        name -> Text,
        local_fs -> Integer,
        remote_fs -> Integer,
        status -> Text,
        state -> Text,
    }
}

diesel::joinable!(filesystems -> nodes (root_node));

diesel::allow_tables_to_appear_in_same_query!(filesystems, nodes, synchros,);
