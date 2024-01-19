cargo lambda build --target aarch64-unknown-linux-gnu
file ./target/lambda/lambda-api/bootstrap
cargo lambda deploy --enable-function-url
