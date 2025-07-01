# Sales-analytics
--вивели усі необхідні аккаунти у  розрізі країн + порахували загальну кількість аккаунтів.
with account_cnts as (
  SELECT
    s.date AS date,
    sp.country AS country,
    a.send_interval AS send_interval,
    a.is_verified AS is_verified,
    a.is_unsubscribed AS is_unsubscribed,
    COUNT(a.id) AS account_cnt
  FROM `DA.account_session` acs
  JOIN `DA.account` a ON acs.account_id = a.id
  JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  GROUP BY 1,2,3,4,5),


--порахували  кількість листів по країнах (відправлених, відкритих та кліки), додали  інтервали
acc_msg as (
    SELECT
    DATE_ADD (s.date, INTERVAL es.sent_date DAY) AS date,
    sp.country AS country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(DISTINCT es.id_message) AS sent_msg,
    COUNT(DISTINCT eo.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM `DA.email_sent` es
  JOIN `DA.account_session` acs ON es.id_account = acs.account_id
  JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  LEFT JOIN `DA.email_open` eo ON es.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` ev ON es.id_message = ev.id_message
  JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  JOIN `DA.account` a ON acs.account_id = a.id
  GROUP BY 1,2,3,4,5),




 --об’єднуємо msg і acc по union all
  union_cnt_and_acc as (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM account_cnts


  UNION ALL


  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 AS account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM acc_msg),


--зводимо дані
united_numbers as (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg
  FROM union_cnt_and_acc
  GROUP BY 1,2,3,4,5),


--сумуємо листи та акаунти по країні
total_counts AS
(  SELECT *,
    SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
    SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
  FROM united_numbers),


--рангуємо за кількістю створених підписників і відправлених листів
total_ranking as  (
  SELECT *,
    DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account,
    DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent
  FROM total_counts)


--фінальний запит
SELECT *
FROM total_ranking
WHERE rank_total_country_account <= 10 OR rank_total_country_sent <= 10
ORDER BY country;
