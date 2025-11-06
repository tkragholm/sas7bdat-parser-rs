#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>

#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/sink/null.hpp>

namespace fs = std::filesystem;

struct BenchmarkStats {
  std::size_t row_count = 0;
  std::size_t column_count = 0;
};

class BenchmarkSink {
public:
  explicit BenchmarkSink(std::shared_ptr<BenchmarkStats> stats)
      : stats_(std::move(stats)) {}

  void set_properties(const cppsas7bdat::Properties &properties) {
    stats_->column_count = properties.column_count;
    inner_.set_properties(properties);
  }

  void push_row(std::size_t index, cppsas7bdat::Column::PBUF row) {
    inner_.push_row(index, row);
    stats_->row_count = index + 1;
  }

  void end_of_data() { inner_.end_of_data(); }

private:
  std::shared_ptr<BenchmarkStats> stats_;
  cppsas7bdat::datasink::null inner_;
};

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: cpp_bench <path-to-sas7bdat>\n";
    return EXIT_FAILURE;
  }

  const fs::path input_path = fs::absolute(argv[1]);
  if (!fs::is_regular_file(input_path)) {
    std::cerr << "Input file not found: " << input_path << '\n';
    return EXIT_FAILURE;
  }

  auto stats = std::make_shared<BenchmarkStats>();
  BenchmarkSink sink(stats);

  try {
    cppsas7bdat::Reader reader(
        cppsas7bdat::datasource::ifstream(input_path.c_str()),
        std::move(sink));

    const auto start = std::chrono::steady_clock::now();
    reader.read_all();
    const auto end = std::chrono::steady_clock::now();

    const std::chrono::duration<double, std::milli> elapsed = end - start;

    std::cout << "File           : " << input_path << '\n';
    std::cout << "Rows processed : " << stats->row_count << '\n';
    std::cout << "Columns        : " << stats->column_count << '\n';
    std::cout << "Elapsed (ms)   : " << elapsed.count() << '\n';
  } catch (const std::exception &ex) {
    std::cerr << "cppsas7bdat error: " << ex.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
