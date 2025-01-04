#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

/* Constants for Meta Commands  */
typedef enum {
  META_COMMAND_SUCCESS,
  META_UNRECOGNIZED_COMMAND
} META_COMMAND_RESULT;

/* Constants for Commmands' Status */
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATE,
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID
} PREPARE_RESULT;

/* Constants for Command type */
typedef enum { COMMAND_SELECT, COMMAND_INSERT } COMMAND_TYPE;

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } EXECUTE_RESULT;

/* Row Structure and Constants :
          Row Structure:
          +----------------+
          | id (4 bytes)   |
          +----------------+
          | username (32)  |
          +----------------+
          | email (255)    |
          +----------------+
*/
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

/* Priting Rows */
inline void printRow(Row *row) {
  std::cout << "ID: " << row->id << ", Username: " << row->username
            << ", Email: " << row->email << "\n";
}

typedef struct {
  COMMAND_TYPE type;
  Row toBeInserted; // only used by INSERT command
} Command;

/* Memory Layout Calculations */
#define size_of_field(structType, field) sizeof(((structType *)0)->field)
const uint32_t ID_SIZE = size_of_field(Row, id);
const uint32_t USERNAME_SIZE = size_of_field(Row, username);
const uint32_t EMAIL_SIZE = size_of_field(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/* Paging System */
const uint32_t PAGE_SIZE = 4096; // 4 KB (common OS page size)
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/* Tables */
typedef struct {
  uint32_t numRows;
  void *pages[TABLE_MAX_PAGES];
} Table;

Table *newTable() {
  Table *table = (Table *)malloc(sizeof(Table));
  table->numRows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
    table->pages[i] = nullptr;
  }
  return table;
}

void freeTable(Table *table) {
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
    if (table->pages[i]) {
      free(table->pages[i]);
    }
  }
  free(table);
}

/* Structure and De-structure Rows */
void structureRow(Row *src, void *dst) {
  std::memcpy(static_cast<uint8_t *>(dst) + ID_OFFSET, &(src->id), ID_SIZE);
  std::memcpy(static_cast<uint8_t *>(dst) + USERNAME_OFFSET, &(src->username),
              USERNAME_SIZE);
  std::memcpy(static_cast<uint8_t *>(dst) + EMAIL_OFFSET, &(src->email),
              EMAIL_SIZE);
}

void destructureRow(void *src, Row *dst) {
  std::memcpy(&(dst->id), static_cast<uint8_t *>(src) + ID_OFFSET, ID_SIZE);
  std::memcpy(&(dst->username), static_cast<uint8_t *>(src) + USERNAME_OFFSET,
              USERNAME_SIZE);
  std::memcpy(&(dst->email), static_cast<uint8_t *>(src) + EMAIL_OFFSET,
              EMAIL_SIZE);
}

/* Page Management */
void *row_slot(Table *table, uint32_t rowNum) {
  uint32_t pageNum = rowNum / ROWS_PER_PAGE;
  uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
  uint32_t byteOffset = rowOffset * ROW_SIZE;

  // Lazy load the page i.e; only load when accessing the page
  void *page = table->pages[pageNum];
  if (page == nullptr) {
    table->pages[pageNum] = malloc(PAGE_SIZE);
    page = table->pages[pageNum];
  }
  return reinterpret_cast<void *>((static_cast<uint8_t *>(page) + byteOffset));
}

/**
 * @brief Displays the default SQLite prompt
 */
inline void displayDefault() { std::cout << "SQLite > "; }

/**
 * @brief Cleanup function called before exit
 */
void closeInput() { std::cout << "Goodbye!\n"; }

/**
 * @brief   Efficiently reads a line of input using std::getline
 * @param   inputBuffer Reference to string where input will be stored
 * @return  Number of bytes read, -1 on error
 */
ssize_t readLine(std::string &inputBuffer) {
  // Clear any existing content for safety
  inputBuffer.clear();

  // Read entire line at once
  if (!std::getline(std::cin, inputBuffer)) {
    // Handle EOF or error
    return -1;
  }

  return inputBuffer.size();
}

META_COMMAND_RESULT selectAndDoMetaCommand(const std::string &inputLine,
                                           Table *table) {
  if (inputLine == ".exit") {
    closeInput();
    freeTable(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_UNRECOGNIZED_COMMAND;
  }
}

/* Matches the command with their type */
PREPARE_RESULT prepareCommand(const std::string &inputLine, Command &command) {
  std::istringstream inputArgStream(inputLine);
  std::string whichCommand;
  inputArgStream >> whichCommand;

  if (whichCommand == "SELECT") {
    command.type = COMMAND_SELECT;
    return PREPARE_SUCCESS;
  } else if (whichCommand == "INSERT") {
    command.type = COMMAND_INSERT;
    std::string usrName, email;
    int64_t id;

    if (!(inputArgStream >> id >> usrName >> email)) {
      return PREPARE_SYNTAX_ERROR;
    }

    if (id < 0) {
      return PREPARE_NEGATIVE_ID;
    }

    if (usrName.size() > COLUMN_USERNAME_SIZE ||
        email.size() >= COLUMN_EMAIL_SIZE) {
      return PREPARE_STRING_TOO_LONG;
    }

    command.toBeInserted.id = id;
    std::strncpy(command.toBeInserted.username, usrName.c_str(),
                 sizeof(command.toBeInserted.username) - 1);
    command.toBeInserted.username[sizeof(command.toBeInserted.username) - 1] =
        '\0';

    std::strncpy(command.toBeInserted.email, email.c_str(),
                 sizeof(command.toBeInserted.username) - 1);
    command.toBeInserted.email[sizeof(command.toBeInserted.email) - 1] = '\0';

    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATE;
}

/* Executing the INSERT command */
EXECUTE_RESULT executeInsertCommand(Command &command, Table &table) {
  if (table.numRows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }
  Row *rowToinsert = &(command.toBeInserted);
  structureRow(rowToinsert, row_slot(&table, table.numRows));
  ++table.numRows;

  return EXECUTE_SUCCESS;
}

/* Executing the SELECT command */
EXECUTE_RESULT executeSelectCommand(Command &command, Table &table) {
  Row row;
  for (uint32_t i = 0; i < table.numRows; ++i) {
    destructureRow(row_slot(&table, i), &row);
    printRow(&row);
  }

  return EXECUTE_SUCCESS;
}

/* Execute the logic behind the command */
EXECUTE_RESULT executeCommand(Command &command, Table &table) {
  switch (command.type) {
  case COMMAND_INSERT:
    return executeInsertCommand(command, table);
  case COMMAND_SELECT:
    return executeSelectCommand(command, table);
  }
}

/**
 * @brief   Main loop for taking input, runs infinte loop, gets a line and
 *          process it
 */
int main(int argc, char **argv) {
  std::string inputLine;
  Table *table = newTable();

  while (true) {
    displayDefault();

    if ((readLine(inputLine)) < 0) {
      if (std::cin.eof()) {
        std::cout << "\n"; // Add newline for EOF
        closeInput();
        return EXIT_SUCCESS;
      }
      std::cerr << "Error reading input\n";
      return EXIT_FAILURE;
    }

    if (!inputLine.size()) {
      std::cout << "Unrecognized Input :\n";
      continue;
    }

    /* Handling Meta commands */
    if (inputLine[0] == '.') {
      switch (selectAndDoMetaCommand(inputLine, table)) {
      case (META_COMMAND_SUCCESS):
        continue;
      case (META_UNRECOGNIZED_COMMAND):
        std::cout << "Unexpected Input: '" << inputLine << "'\n";
        continue;
      }
    }

    /* Handling Non-Meta commands */
    Command command;
    switch (prepareCommand(inputLine, command)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_SYNTAX_ERROR:
      std::cout << "Syntax error. Could not parse command.\n";
      continue;
    case PREPARE_STRING_TOO_LONG:
      std::cout << "String too long. Could not insert.\n";
      continue;
    case PREPARE_NEGATIVE_ID:
      std::cout << "Negative ID. Could not insert.\n";
      continue;
    case PREPARE_UNRECOGNIZED_STATE:
      std::cout << "Unrecognized keyword in '" << inputLine << "'\n";
      continue;
    }

    /* execute the command */
    switch (executeCommand(command, *table)) {
    case EXECUTE_SUCCESS:
      std::cout << "Executed\n";
      break;
    case EXECUTE_TABLE_FULL:
      std::cout << "Error: Table full.\n";
      break;
    }
  }

  std::cout << "Unknown error occurred. Exiting...\n";
  return EXIT_FAILURE;
}
