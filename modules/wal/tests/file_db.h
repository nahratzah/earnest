#pragma once

#include <cstddef>
#include <filesystem>
#include <string_view>
#include <vector>
#include <earnest/dir.h>

extern earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir;

auto str_to_byte_vector(std::string_view s) -> std::vector<std::byte>;
