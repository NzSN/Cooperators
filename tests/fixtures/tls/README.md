# Disposable TLS fixtures

These certificates and the server private key are public test data. They are
only for loopback integration fixtures and must never be used in a deployment.
The leaf certificate is valid for `localhost`; connecting to its IP address
tests hostname rejection. The CA private key was not retained in the repository.
