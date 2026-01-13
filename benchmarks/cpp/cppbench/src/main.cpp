#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
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

class CsvSnapshotSink {
public:
  explicit CsvSnapshotSink(const fs::path &output_path)
      : out_(output_path, std::ios::out | std::ios::trunc) {
    out_.setf(std::ios::fmtflags(0), std::ios::floatfield);
    out_ << std::setprecision(17);
  }

  void set_properties(const cppsas7bdat::Properties &properties) {
    columns_ = cppsas7bdat::COLUMNS(properties.columns);
    bool first = true;
    for (const auto &column : columns_) {
      if (first)
        first = false;
      else
        out_ << ',';
      write_string(column.name);
    }
    out_ << '\n';
  }

  void push_row(std::size_t /*index*/, cppsas7bdat::Column::PBUF row) {
    bool first = true;
    for (const auto &column : columns_) {
      if (first)
        first = false;
      else
        out_ << ',';

      switch (column.type) {
      case cppsas7bdat::Column::Type::string:
        write_string(column.get_string(row));
        break;
      case cppsas7bdat::Column::Type::integer:
        out_ << column.get_integer(row);
        break;
      case cppsas7bdat::Column::Type::number: {
        const auto value = column.get_number(row);
        if (!std::isnan(value)) {
          out_ << value;
        }
        break;
      }
      case cppsas7bdat::Column::Type::datetime:
        write_datetime(column.get_datetime(row));
        break;
      case cppsas7bdat::Column::Type::date:
        write_date(column.get_date(row));
        break;
      case cppsas7bdat::Column::Type::time:
        write_time(column.get_time(row));
        break;
      case cppsas7bdat::Column::Type::unknown:
        break;
      }
    }
    out_ << '\n';
  }

  void end_of_data() {}

private:
  void write_string(std::string_view value) {
    out_ << '"';
    for (const auto ch : value) {
      if (ch == '"')
        out_ << "\"\"";
      else
        out_ << ch;
    }
    out_ << '"';
  }

  void write_datetime(const cppsas7bdat::DATETIME &dt) {
    if (dt.is_not_a_date_time() || dt.is_special())
      return;
    const auto duration = dt - epoch();
    const auto micros = duration.total_microseconds();
    out_ << (static_cast<double>(micros) / 1e6);
  }

  void write_date(const cppsas7bdat::DATE &date) {
    if (date.is_not_a_date())
      return;
    const auto days = (date - epoch_date()).days();
    out_ << static_cast<double>(days);
  }

  void write_time(const cppsas7bdat::TIME &time) {
    if (time.is_special())
      return;
    const auto micros = time.total_microseconds();
    out_ << (static_cast<double>(micros) / 1e6);
  }

  static const boost::posix_time::ptime &epoch() {
    static const boost::posix_time::ptime epoch_time(
        boost::gregorian::date(1960, 1, 1));
    return epoch_time;
  }

  static const boost::gregorian::date &epoch_date() {
    static const boost::gregorian::date epoch_date(1960, 1, 1);
    return epoch_date;
  }

  std::ofstream out_;
  cppsas7bdat::COLUMNS columns_;
};

int main(int argc, char *argv[]) {
  if (argc != 2 && argc != 4) {
    std::cerr << "Usage: cpp_bench <path-to-sas7bdat>\n"
              << "       cpp_bench --csv <output.csv> <path-to-sas7bdat>\n";
    return EXIT_FAILURE;
  }

  bool csv_mode = false;
  fs::path output_path;
  fs::path input_path;
  if (argc == 4 && std::string_view(argv[1]) == "--csv") {
    csv_mode = true;
    output_path = fs::absolute(argv[2]);
    input_path = fs::absolute(argv[3]);
  } else if (argc == 2) {
    input_path = fs::absolute(argv[1]);
  } else {
    std::cerr << "Invalid arguments. Expected --csv <output.csv> <input>\n";
    return EXIT_FAILURE;
  }

  if (!fs::is_regular_file(input_path)) {
    std::cerr << "Input file not found: " << input_path << '\n';
    return EXIT_FAILURE;
  }

  try {
    if (csv_mode) {
      CsvSnapshotSink sink(output_path);
      cppsas7bdat::Reader reader(
          cppsas7bdat::datasource::ifstream(input_path.c_str()),
          std::move(sink));
      reader.read_all();
    } else {
      auto stats = std::make_shared<BenchmarkStats>();
      BenchmarkSink sink(stats);
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
    }
  } catch (const std::exception &ex) {
    std::cerr << "cppsas7bdat error: " << ex.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
