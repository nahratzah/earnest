#ifndef EARNEST_ISOLATION_H
#define EARNEST_ISOLATION_H

namespace earnest {


/**
 * \brief Transaction isolation levels.
 * \details
 * Describes the isolation level of a transaction.
 */
enum class isolation {
  /**
   * \brief Read-commited transaction isolation.
   * \details
   * Read commited data. New commits become visible immediately.
   */
  read_commited,

  /**
   * \brief Repeatable-read transaction isolation.
   * \details
   * Point-in-time read. New commits won't be visible.
   */
  repeatable_read,

  /**
   * \brief Serializable transaction isolation.
   * \details
   * Point-in-time read. New commits won't be visible.
   *
   * Additionally, the transaction will fail to commit,
   * if any of its reads or writes has been written to
   * by another transaction.
   */
  serializable
};


} /* namespace earnest */

#endif /* EARNEST_ISOLATION_H */
