CREATE TABLE transactions_v2 (
         transaction_id Uint64,
         user_id Uint64,
         transaction_date Date,
         amount Double,
         merchant_id Uint64,
         category Utf8,
         PRIMARY KEY (transaction_id)
     );