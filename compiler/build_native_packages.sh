#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "$script_dir/.." && pwd)
mbedtls_dir="$repo_dir/modules/tls/vendor/mbedtls"
package_build_dir="$repo_dir/build/native-packages"
mbedtls_build_dir="$package_build_dir/mbedtls"
archive_path="$package_build_dir/libtyrion_tls.a"
bridge_object="$package_build_dir/tyrion_tls.o"
payload_dir="$repo_dir/extensions/toolchain/build"
payload_path="$payload_dir/payload.S"

if [[ ! -f "$mbedtls_dir/LICENSE" ]]; then
  echo "missing vendored Mbed TLS source: $mbedtls_dir" >&2
  exit 1
fi

mkdir -p "$package_build_dir" "$payload_dir"

cmake \
  -S "$mbedtls_dir" \
  -B "$mbedtls_build_dir" \
  -DENABLE_PROGRAMS=OFF \
  -DENABLE_TESTING=OFF \
  -DMBEDTLS_FATAL_WARNINGS=OFF \
  -DCMAKE_BUILD_TYPE=MinSizeRel \
  -DCMAKE_C_FLAGS="-ffunction-sections -fdata-sections"
cmake --build "$mbedtls_build_dir" --parallel

cc \
  -Os \
  -Wall \
  -Wextra \
  -Werror \
  -ffunction-sections \
  -fdata-sections \
  -I "$mbedtls_dir/include" \
  -c "$repo_dir/modules/tls/src/tyrion_tls.c" \
  -o "$bridge_object"

libraries=(
  "$mbedtls_build_dir/library/libmbedtls.a"
  "$mbedtls_build_dir/library/libmbedx509.a"
  "$mbedtls_build_dir/library/libmbedcrypto.a"
  "$mbedtls_build_dir/3rdparty/everest/libeverest.a"
  "$mbedtls_build_dir/3rdparty/p256-m/libp256m.a"
)

if [[ $(uname -s) == Darwin ]]; then
  libtool -static -o "$archive_path" "$bridge_object" "${libraries[@]}"
  symbol_prefix="_"
  section='.section __DATA,__const'
else
  mri_path="$package_build_dir/archive.mri"
  {
    printf 'CREATE %s\n' "$archive_path"
    printf 'ADDMOD %s\n' "$bridge_object"
    for library in "${libraries[@]}"; do
      printf 'ADDLIB %s\n' "$library"
    done
    printf 'SAVE\nEND\n'
  } >"$mri_path"
  ar -M <"$mri_path"
  ranlib "$archive_path"
  symbol_prefix=""
  section='.section .rodata'
fi

escaped_archive=${archive_path//\\/\\\\}
escaped_archive=${escaped_archive//\"/\\\"}
{
  printf '%s\n' "$section"
  printf '.balign 16\n'
  printf '.globl %styrion_toolchain_tls_start\n' "$symbol_prefix"
  printf '%styrion_toolchain_tls_start:\n' "$symbol_prefix"
  printf '.incbin "%s"\n' "$escaped_archive"
  printf '.globl %styrion_toolchain_tls_end\n' "$symbol_prefix"
  printf '%styrion_toolchain_tls_end:\n' "$symbol_prefix"
  if [[ $(uname -s) != Darwin ]]; then
    printf '.section .note.GNU-stack,"",@progbits\n'
  fi
} >"$payload_path"

printf 'native_package=tls\narchive=%s\nbytes=%s\npayload=%s\n' \
  "$archive_path" \
  "$(wc -c <"$archive_path" | tr -d ' ')" \
  "$payload_path"
