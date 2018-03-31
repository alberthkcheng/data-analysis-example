-- We only care about transactions and products instead of customers and quantity
WITH transactions AS(
  SELECT DISTINCT OrderID, ProductID 
  FROM sample_orders
  ORDER BY OrderID
), total_transactions AS(
  SELECT COUNT(DISTINCT OrderID) AS TotalTransaction FROM transactions
), product_frequency AS(
  SELECT ProductID, COUNT(*) AS Frequency
  FROM transactions
  GROUP BY ProductID
  ORDER BY Frequency DESC
), bestsellers AS(
  SELECT *
  FROM product_frequency
  LIMIT 10
), basket_rules AS(
  SELECT transactionsA.ProductID AS ProductA, transactionsB.ProductID AS ProductB, count(*) AS Occurrences
  FROM transactions AS transactionsA
  JOIN transactions AS transactionsB 
  ON transactionsA.OrderID == transactionsB.OrderID AND transactionsA.ProductID <> transactionsB.ProductID
  GROUP BY transactionsA.ProductID, transactionsB.ProductID
  ORDER BY count(*) DESC
)
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
HAVING Support >= 0.0002 AND Confidence <= 0.14 AND LiftRatio >= 50
-- HAVING Support >= 0.2 AND Confidence >= 0.6 AND LiftRatio > 1
