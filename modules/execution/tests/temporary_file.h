#pragma once

#include <string>
#include <string_view>

struct temporary_file {
  temporary_file();
  ~temporary_file();

  temporary_file(temporary_file&& other) noexcept
  : fd(std::exchange(other.fd, -1)),
    filename(std::exchange(other.filename, std::string()))
  {}

  temporary_file(const temporary_file&) = delete;

  auto get_contents() const -> std::string;
  auto set_contents(std::string_view) -> void;

  int fd = -1;
  std::string filename;
};
