#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>

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

typedef enum { NODE_INTERNAL, NODE_LEAF } nodeType;

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
const uint32_t PAGE_SIZE = 4096;  // 4 KB (common OS page size)
#define TABLE_MAX_PAGES 100

/**
 * @brief Common Node header layout
 * @details Nodes need to store some metadata in a header at the beginning of
 * the page. Every node will store what type of node it is, whether or not it is
 * the root node, & a pointer to its parent (for finding a node's siblings).
 * @note Each node will correspond to one page. Total: 1 + 1 + 4 = 6 bytes
 * (COMMON_NODE_HEADER_SIZE)
 *
 * @example
 *        +-----------------------------+  ← Offset 0
 *        | Node Type (1 byte)          |  ← Offset 0 ... 0
 *        +-----------------------------+
 *        | Is Root (1 byte)            |  ← Offset 1 ... 1
 *        +-----------------------------+
 *        | Parent Pointer (4 bytes)    |  ← Offset 2 ... 5
 *        +-----------------------------+
 */

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * @brief Leaf node header layout
 * @details These nodes need to store how many "cells" they contain
 * @note A "cell" is a key-value pair. Total: 4 bytes
 * (LEAF_NODE_NUM_CELLS_SIZE). Combined header for a leaf node is 6 + 4 = 10
 * bytes (LEAF_NODE_HEADER_SIZE)
 * @example
 *       +-----------------------------+  ← Offset 6 (COMMON_NODE_HEADER_SIZE)
 *       | Number of Cells (4 bytes)   |  ← Offset 6 ... 9
 *       +-----------------------------+
 * @
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/**
 * @brief Leaf node body
 * @details Leaf node is an array of cells. Each cell is a key followed by a
 * value (a serialised row)
 * @note Total cell size: 4 + ROW_SIZE bytes (LEAF_NODE_CELL_SIZE)
 * @example
 *      +-----------------------------+  ← Within cell: Offset 0
 *      | Key (4 bytes)               |  ← Offset 0 ... 3
 *      +-----------------------------+
 *      | Value (ROW_SIZE bytes)      |  ← Offset 4 ... (4 + ROW_SIZE - 1)
 *      +-----------------------------+
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =

    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

/* Forward declarations */
typedef struct Pager Pager;  // Forward declaration
void *getPage(Pager *pager, uint32_t pageNum);

/**
 * @brief Takes page number \p x and it \return block of memory containing the
 *        page.
 * @note  It looks into Cache first, but on Cache miss, it copies data from disk
 *        into memory by reading the database file.
 */
struct Pager {
  int fd;
  uint32_t fileSize;
  uint32_t numPages;
  void *pages[TABLE_MAX_PAGES];
};

/* Tables */
typedef struct {
  uint32_t numRows;
  uint32_t rootPageNum;
  Pager *pager;
} Table;

/* Represents location in the Table */
typedef struct {
  Table *table;
  uint32_t pageNum;
  uint32_t cellNum;
  bool endOfTable;  // Indicates a position one before the last element
} Cursor;

/**
 * @brief Functions for accessing Node and it's values.
 * @example
 *      +---------------------------------------------------+
 *      | Common Node Header (6 bytes)                      |
 *      |   - Node Type (1 byte)                            |
 *      |   - Is Root (1 byte)                              |
 *      |   - Parent Pointer (4 bytes)                      |
 *      +---------------------------------------------------+
 *      | Leaf Node Header (4 bytes)                        |
 *      |   - Number of Cells (4 bytes)                     |
 *      +---------------------------------------------------+
 *      | Leaf Node Body (Array of Cells)                   |
 *      |   ┌─────────────────────────┐                     |
 *      |   | Cell 0:                 |                     |
 *      |   |   - Key (4 bytes)       |                     |
 *      |   |   - Value (ROW_SIZE)    |                     |
 *      |   ├─────────────────────────┤                     |
 *      |   | Cell 1:                 |                     |
 *      |   |   - Key (4 bytes)       |                     |
 *      |   |   - Value (ROW_SIZE)    |                     |
 *      |   ├─────────────────────────┤       ...           |
 *      |   | Cell n:                 |                     |
 *      |   |   - Key (4 bytes)       |                     |
 *      |   |   - Value (ROW_SIZE)    |                     |
 *      |   └─────────────────────────┘                     |
 *      +---------------------------------------------------+
 */

uint32_t *leafNodeNumCells(void *node) {
  return (uint32_t *)((char *)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void *leafNodeCell(void *node, uint32_t cellNum) {
  return (char *)node + LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t *leafNodeKey(void *node, uint32_t cellNum) {
  return (uint32_t *)leafNodeCell(node, cellNum);
}

void *leafNodeValue(void *node, uint32_t cellNum) {
  return (char *)leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}

void initializeLeafNode(void *node) {
  *leafNodeNumCells(node) = 0;
  // Set node type to leaf
  *(uint8_t *)((char *)node + NODE_TYPE_OFFSET) =
      NODE_LEAF;  // Added: set node type
}

/* Priting Rows */
inline void printRow(Row *row) {
  std::cout << "ID: " << row->id << ", Username: " << row->username
            << ", Email: " << row->email << "\n";
}

void printConstants() {
  std::cout << "ROW_SIZE : " << ROW_SIZE << "\n";
  std::cout << "COMMON_NODE_HEADER_SIZE : " << (int)COMMON_NODE_HEADER_SIZE
            << "\n";
  std::cout << "LEAF_NODE_HEADER_SIZE : " << LEAF_NODE_HEADER_SIZE << "\n";
  std::cout << "LEAF_NODE_CELL_SIZE : " << LEAF_NODE_CELL_SIZE << "\n";
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS : " << LEAF_NODE_SPACE_FOR_CELLS
            << "\n";
  std::cout << "LEAF_NODE_MAX_CELLS : " << LEAF_NODE_MAX_CELLS << "\n";
}

void printLeafNode(void *node) {
  uint32_t numCells = *leafNodeNumCells(node);
  std::cout << "Leaf (Size : " << numCells << ")" << "\n";
  for (uint32_t i = 0; i < numCells; ++i) {
    uint32_t key = *leafNodeKey(node, i);
    std::cout << "    - " << i << " : " << key << "\n";
  }
}

typedef struct {
  COMMAND_TYPE type;
  Row toBeInserted;  // only used by INSERT command
} Command;

/**
 * @brief Opens DB file and keeps track of its size. Also initialize the page
 *        cache to all null
 *
 * @param fileName
 * @return Pager*
 */
Pager *pagerOpen(const std::string &fileName) {
  int fileDesc = open(fileName.c_str(),
                      O_RDWR |      // Read/Write mode
                          O_CREAT,  // Create file if it doesn't exist
                      S_IWUSR |     // User write permission
                          S_IRUSR   // User read permission
  );

  if (fileDesc == -1) {
    std::cerr << "Unable to open file " << fileName << '\n';
    exit(EXIT_FAILURE);
  }

  off_t fileLength = lseek(fileDesc, 0, SEEK_END);

  Pager *pager = (Pager *)malloc(sizeof(Pager));
  pager->fd = fileDesc;
  pager->fileSize = fileLength;
  pager->numPages = (fileLength / PAGE_SIZE);

  if (fileLength % PAGE_SIZE != 0 && fileLength > 0) {
    std::cerr
        << "DB file doesn't have whole number of Pages. Corrupted file.\n";
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
    pager->pages[i] = nullptr;
  }
  return pager;
}

/**
 * @brief Handles cache miss of the asked page \p pageNum
 *
 * @param pager
 * @param pageNum
 * @return void pointer to the page
 */
void *getPage(Pager *pager, uint32_t pageNum) {
  if (pageNum > TABLE_MAX_PAGES) {
    std::cout << "Tried to fetch page out of bounds. " << pageNum << " > "
              << TABLE_MAX_PAGES << '\n';
    exit(EXIT_FAILURE);
  }

  // Cache Miss. Allocate memory & load from file
  if (pager->pages[pageNum] == nullptr) {
    void *page = malloc(PAGE_SIZE);
    uint32_t noOfPages = pager->fileSize / PAGE_SIZE;

    // We might need to read a partial page at the end of the file
    if (pageNum <= noOfPages) {
      lseek(pager->fd, pageNum * PAGE_SIZE, SEEK_SET);
      ssize_t bytes = read(pager->fd, page, PAGE_SIZE);
      if (bytes < 0) {
        std::cerr << "Error reading file: " << std::strerror(errno) << '\n';
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[pageNum] = page;

    if (pageNum >= pager->numPages) {
      pager->numPages = pageNum + 1;
    }
  }
  return pager->pages[pageNum];
}

Table *dbOpen(const std::string &fileName) {
  Pager *pager = pagerOpen(fileName);

  Table *table = (Table *)malloc(sizeof(Table));
  table->pager = pager;
  table->rootPageNum = 0;

  if (pager->numPages == 0) {  // New DB file. Initialize page 0 as leaf node.
    void *rootNode = getPage(pager, 0);
    initializeLeafNode(rootNode);
  }

  return table;
}

/* Create new Cursor for the Start of Table */
Cursor *tableStart(Table *table) {
  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->pageNum = table->rootPageNum;
  cursor->endOfTable = false;
  void *rootNode = getPage(table->pager, table->rootPageNum);
  uint32_t numCells = *leafNodeNumCells(rootNode);
  cursor->cellNum = 0;

  // If table is empty, set endOfTable to true
  if (numCells == 0) {
    cursor->endOfTable = true;
  }

  return cursor;
}

/* Create new Cursor for the End of Table */
Cursor *tableEnd(Table *table) {
  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->pageNum = table->rootPageNum;

  void *rootNode = getPage(table->pager, table->rootPageNum);
  uint32_t numCells = *leafNodeNumCells(rootNode);
  cursor->cellNum = numCells;
  cursor->endOfTable = true;

  return cursor;
}

void cursorAdvance(Cursor *cursor) {
  void *node = getPage(cursor->table->pager, cursor->pageNum);
  cursor->cellNum += 1;

  if (cursor->cellNum >= *leafNodeNumCells(node)) {
    cursor->endOfTable = true;
  }
}

/* Structure and De-structure Rows */
void structureRow(Row *src, void *dst) {
  memcpy((char *)dst + ID_OFFSET, &(src->id), ID_SIZE);
  memcpy((char *)dst + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
  memcpy((char *)dst + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void destructureRow(void *src, Row *dst) {
  memcpy(&(dst->id), (char *)src + ID_OFFSET, ID_SIZE);
  memcpy(&(dst->username), (char *)src + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(dst->email), (char *)src + EMAIL_OFFSET, EMAIL_SIZE);
}

/* Page Management */
void *cursorValue(Cursor *cursor) {
  uint32_t pageNum = cursor->pageNum;
  void *page = getPage(cursor->table->pager, pageNum);
  return leafNodeValue(page, cursor->cellNum);
}

void pagerFlush(Pager *pager, uint32_t pageNum) {
  if (pager->pages[pageNum] == nullptr) {
    std::cerr << "Tried to flush unallocated page\n";
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->fd, pageNum * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    std::cerr << "Error seeking to page: " << std::strerror(errno) << '\n';
    exit(EXIT_FAILURE);
  }
  ssize_t bytes = write(pager->fd, pager->pages[pageNum], PAGE_SIZE);
  if (bytes < 0) {
    std::cerr << "Error writing to file: " << std::strerror(errno) << '\n';
    exit(EXIT_FAILURE);
  }
}

void leafNodeInsert(Cursor *cursor, uint32_t key, Row *value) {
  void *node = getPage(cursor->table->pager, cursor->pageNum);
  uint32_t numCells = *leafNodeNumCells(node);

  if (numCells >= LEAF_NODE_MAX_CELLS) {  // Node is Full.
    std::cout << "Need to implement splitting a leaf node.\n";
    exit(EXIT_FAILURE);
  }

  if (cursor->cellNum < numCells) {  // Make room for new cell.
    for (uint32_t i = numCells; i > cursor->cellNum; --i) {
      memcpy(leafNodeCell(node, i), leafNodeCell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  *leafNodeNumCells(node) += 1;
  *leafNodeKey(node, cursor->cellNum) = key;
  structureRow(value, leafNodeValue(node, cursor->cellNum));
}

/**
 * @brief Flushes the page cache to disk, closes the DB file, frees the Pager &
 *        Table structures
 *
 * @param table pointer to the table structure
 * @note  Wait to flush the cache to disk until the user closes the DB
 *        connection.
 */
void dbClose(Table *table) {
  Pager *pager = table->pager;

  for (uint32_t i = 0; i < pager->numPages; ++i) {
    if (pager->pages[i] == nullptr) continue;
    pagerFlush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = nullptr;
  }

  int result = close(pager->fd);
  if (result == -1) {
    std::cerr << "Error closing DB file: " << std::strerror(errno) << '\n';
    exit(EXIT_FAILURE);
  }
  free(pager);
  free(table);
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
    dbClose(table);
    closeInput();
    exit(EXIT_SUCCESS);
  } else if (inputLine == ".btree") {
    std::cout << "Tree :\n";
    printLeafNode(getPage(table->pager, 0));
    return META_COMMAND_SUCCESS;
  } else if (inputLine == ".constants") {
    std::cout << "Constants :\n";
    printConstants();
    return META_COMMAND_SUCCESS;
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
        email.size() > COLUMN_EMAIL_SIZE) {
      return PREPARE_STRING_TOO_LONG;
    }

    command.toBeInserted.id = id;
    strncpy(command.toBeInserted.username, usrName.c_str(),
            COLUMN_USERNAME_SIZE);
    command.toBeInserted.username[COLUMN_USERNAME_SIZE] = '\0';

    strncpy(command.toBeInserted.email, email.c_str(), COLUMN_EMAIL_SIZE);
    command.toBeInserted.email[COLUMN_EMAIL_SIZE] = '\0';

    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATE;
}

/* Executing the INSERT command */
EXECUTE_RESULT executeInsertCommand(Command &command, Table &table) {
  void *node = getPage(table.pager, table.rootPageNum);
  if (*leafNodeNumCells(node) >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }
  Row *rowToinsert = &(command.toBeInserted);
  Cursor *cursor = tableEnd(&table);
  leafNodeInsert(cursor, rowToinsert->id, rowToinsert);
  free(cursor);

  return EXECUTE_SUCCESS;
}

/* Executing the SELECT command */
EXECUTE_RESULT executeSelectCommand(Command &command, Table &table) {
  Cursor *cursor = tableStart(&table);
  Row row;

  while (!(cursor->endOfTable)) {
    destructureRow(cursorValue(cursor), &row);
    printRow(&row);
    cursorAdvance(cursor);
  }

  free(cursor);

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
  if (argc < 2) {
    std::cerr << "Must provide a DB filename.\n";
    exit(EXIT_FAILURE);
  }

  std::string inputLine;
  std::string filename = argv[1];
  Table *table = dbOpen(filename);

  while (true) {
    displayDefault();

    if ((readLine(inputLine)) < 0) {
      if (std::cin.eof()) {
        std::cout << "\n";  // Add newline for EOF
        closeInput();
        dbClose(table);
        return EXIT_SUCCESS;
      }
      std::cerr << "Error reading input\n";
      dbClose(table);
      return EXIT_FAILURE;
    }

    if (inputLine.empty()) {
      std::cout << "Unrecognized Input\n";
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
  dbClose(table);
  return EXIT_FAILURE;
}