use herdr_connect_rs::tab_list;

#[test]
fn tab_list_contract_is_named() {
    assert_eq!(tab_list(), vec!["tab.list".to_string()]);
}
