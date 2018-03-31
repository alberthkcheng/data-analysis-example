WITH transactions AS(
  -- For basket analysis, we only care about transactions and products instead of customers and quantity
  SELECT DISTINCT OrderID, ProductID 
  FROM sample_orders
  ORDER BY OrderID
), 
total_transactions AS(
  -- Compute total transaction for calculating Support
  SELECT COUNT(DISTINCT OrderID) AS TotalTransaction FROM transactions
), 
product_frequency AS(
  -- Compute number of times a product appear in transactions
  SELECT ProductID, COUNT(*) AS Frequency
  FROM transactions
  GROUP BY ProductID
  ORDER BY Frequency DESC
), 
bestsellers AS(
  -- Get top 10 best sellers
  SELECT *
  FROM product_frequency
  LIMIT 10
), 
bestsellers_transaction AS(
  --  Optimization - only get orders which contains top 10 best seller before cross join
  -- Get top 10 best sellers
  SELECT transactions.*
  FROM transactions
  JOIN (SELECT DISTINCT OrderID FROM transactions JOIN bestsellers ON transactions.ProductID == bestsellers.ProductID) AS bestsellers_order
  ON transactions.OrderID == bestsellers_order.OrderID
), 
basket_rules AS(
  -- Generate association rules by cross joining product within each transaction
  SELECT transactionsA.ProductID AS ProductA, transactionsB.ProductID AS ProductB, count(*) AS Occurrences
  FROM bestsellers_transaction AS transactionsA
  JOIN bestsellers_transaction AS transactionsB 
  ON transactionsA.OrderID == transactionsB.OrderID AND transactionsA.ProductID <> transactionsB.ProductID
  GROUP BY transactionsA.ProductID, transactionsB.ProductID
  ORDER BY count(*) DESC
)
-- SELECT * FROM transactions limit 20 -- Toggle comment to test
-- SELECT * FROM total_transactions limit 20 -- Toggle comment to test
-- SELECT * FROM product_frequency limit 20 -- Toggle comment to test
-- SELECT * FROM bestsellers limit 20 -- Toggle comment to test
-- SELECT * FROM bestsellers_transaction -- limit 20 -- Toggle comment to test
-- SELECT * FROM basket_rules -- limit 20 -- Toggle comment to test

SELECT ProductA, ProductB, Occurrences, 
  Occurrences/total_transactions.TotalTransaction AS Support,
  Occurrences/bestsellers.Frequency AS Confidence,
  (Occurrences*total_transactions.TotalTransaction)/(bestsellers.Frequency*product_frequency.Frequency) AS LiftRatio,
  total_transactions.TotalTransaction--, 
  -- bestsellers.Frequency AS FrequencyA, product_frequency.Frequency AS FrequencyB
FROM basket_rules
JOIN bestsellers
ON basket_rules.ProductA == bestsellers.ProductID
JOIN product_frequency
ON basket_rules.ProductB == product_frequency.ProductID
CROSS JOIN total_transactions
HAVING Support >= 0.2 AND Confidence >= 0.6 AND LiftRatio > 1
-- No rules with support >=0.2, below is alternative parameter for testing
-- HAVING Support >= 0.0002 AND Confidence <= 0.14 AND LiftRatio >= 50 
