use herdr_connect_rs::tab_list;

#[test]
fn tab_list_contract_is_named() {
    let tabs = tab_list();
    assert!(!tabs.iter().any(|tab| tab == "tab.list"));
}
