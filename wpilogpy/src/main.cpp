#include <fmt/format.h>
#include <string>
#include <memory>
#include <unordered_map>
#include <wpi/DataLogReader.h>
#include <wpi/MemoryBuffer.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

struct Column {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    std::string name;

    Column(std::string name, std::unique_ptr<arrow::ArrayBuilder> builder) {
        this->builder = std::move(builder);
        this->name = name;
    }

    void reserve(int64_t size) {
        auto result = builder->Reserve(size);
    }

    static std::unique_ptr<Column> make(std::string_view name, std::string_view const& type) {
        return std::make_unique<Column>(std::string(name), std::make_unique<arrow::Int64Builder>());
    }
    
};

int main() {
    fmt::print("Starting\n");
    std::string filename = "C:\\Users\\wtoth\\Desktop\\2025txwaclogs\\akit_25-03-01_17-17-43_txwac_e11.wpilog";
    auto fileBuffer = wpi::MemoryBuffer::GetFile(filename);
    if (!fileBuffer) {
        fmt::print("Unable to load file {}", filename);
        return -1;
    }

    wpi::log::DataLogReader reader{std::move(*fileBuffer)};
    if (!reader.IsValid()) {
        fmt::print("Invalid WPILog {}", filename);
        return -1;
    }

    std::unordered_map<int, std::unique_ptr<Column>> entries;

    fmt::print("Starting\n");
    int64_t rows = 0;
    for (auto& itr : reader) {
        if (itr.IsStart()) {
            wpi::log::StartRecordData start_record;
            itr.GetStartData(&start_record);
            entries[start_record.entry] = Column::make(start_record.name, start_record.type);
        } else if (itr.IsFinish()) {
            // TODO:
            fmt::print("Recieved Finish for {}, skipping", itr.GetEntry());
        } else if (itr.IsSetMetadata()) {
            fmt::print("Recieved SetMetadata for {}, skipping", itr.GetEntry());
        } else if (itr.IsControl()) {
            fmt::print("Recieved Invalid Control for {}, skipping", itr.GetEntry());
        } else {
            // IsData
            rows++;

            // TODO: Parse out structschema
        }
        //fmt::print("ID: {}", itr.GetEntry());
    }

    // for (auto& [id, column] : entries) {
    //     column.reserve(rows);
    // }

    // for (auto& itr : reader) {
    //     if (!itr.IsControl()) {
    //         auto entry = entries[itr.GetEntry()];
    //     }
    // }

    fmt::print("Hello {}\n", rows);
    return 0;
}