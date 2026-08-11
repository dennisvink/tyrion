# Tyrion TLS native module

The TLS transport uses the vendored Mbed TLS source under
`vendor/mbedtls`. Release builds compile the source once per native target
and embed the resulting `libtyrion_tls.a` toolchain resource in `tyrionic`.
Applications link that archive when their module graph uses TLS; application
builds do not recompile Mbed TLS.

Mbed TLS 3.6.7 is pinned by `VERSION` and is distributed under its upstream
Apache-2.0 OR GPL-2.0-or-later license. Tyrion consumes it under
Apache-2.0.
