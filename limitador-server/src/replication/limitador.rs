pub mod service {
    pub mod replication {
        // clippy will barf on protobuff generated code for enum variants in
        // v3::socket_option::SocketState, so allow this lint
        #[allow(clippy::enum_variant_names, clippy::derive_partial_eq_without_eq)]
        pub mod v1 {
            tonic::include_proto!("limitador.service.replication.v1");
        }
    }
}
