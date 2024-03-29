#include "file_db.h"

#include <UnitTest++/UnitTest++.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <string_view>
#include <vector>
#include <earnest/dir.h>

earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir {
  using namespace std::literals;

  std::cerr << "ensure_dir_exists_and_is_empty(" << name << ")\n";
  if (name.has_parent_path())
    throw std::runtime_error("directory '"s + name.string() + "' is not a relative name"s);

  bool exists = false;
  for (auto d : write_dir) {
    if (d.path() == name) exists = true;
  }

  earnest::dir new_dir;
  if (!exists)
    new_dir.create(write_dir, name);
  else
    new_dir.open(write_dir, name);

  for (auto file : new_dir) {
    if (file.path() == "." || file.path() == "..") continue;

    try {
      new_dir.erase(file);
    } catch (const std::exception& e) {
      std::cerr << "error cleaning dir'" << name << "', failed to erase '" << file.path() << "': " << e.what() << std::endl;
      throw;
    }
  }
  return new_dir;
}

auto str_to_byte_vector(std::string_view s) -> std::vector<std::byte> {
  std::vector<std::byte> v;
  std::transform(s.begin(), s.end(),
      std::back_inserter(v),
      [](char c) -> std::byte {
        return static_cast<std::byte>(static_cast<unsigned char>(c));
      });
  return v;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << (argc > 0 ? argv[0] : "wal_test") << " writeable_dir\n"
        <<  "writeable_dir: points at a directory where we can write files\n";
    return 1;
  }
  try {
    write_dir = earnest::dir(argv[1]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
