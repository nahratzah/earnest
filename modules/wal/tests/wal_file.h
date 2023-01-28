#pragma once

#include <filesystem>
#include <earnest/dir.h>

extern earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir;
