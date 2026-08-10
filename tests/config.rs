use herdr_connect_rs::load_config;

#[test]
fn resolves_reference_environment_table() {
    let cases = [
        (
            [("HERDR_SOCKET_PATH", "/tmp/herdr-test.sock")],
            "/tmp/herdr-test.sock",
        ),
        ([("UNSET", "")], "/home/test/.config/herdr/herdr.sock"),
    ];
    for (environment, expected_socket) in cases {
        let actual = load_config(&environment, "/home/test");
        assert_eq!(actual.herdr_socket_path, expected_socket);
        assert_eq!(actual.poll_interval_ms, 1_500);
    }
}
