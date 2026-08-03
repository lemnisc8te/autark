use libautark::demo;

#[tokio::main]
pub async fn main() {
    // Start the console subscriber
    console_subscriber::init();

    let handle = tokio::runtime::Handle::current();

    demo().await;
}
