-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  演示脚本:CDC 实时镜像 + 视图简化复杂查询(客户-订单-明细)            ║
-- ║  分两段执行:                                                        ║
-- ║   【第一段】主库(PostgreSQL console):建表 + 插入数据                ║
-- ║   【第二段】湖侧(DuckDB console):建视图 + 查询演示                  ║
-- ║  第一段执行后几秒,三张表自动镜像到 lake.public.*,再执行第二段        ║
-- ╚══════════════════════════════════════════════════════════════════╝


-- ████████████████████████████████████████████████████████████████████
-- 【第一段】在 PostgreSQL 主库 console 执行(jdbc:postgresql://...:15432)
-- ████████████████████████████████████████████████████████████████████

-- 每表都带主键——完整镜像(UPDATE/DELETE 跟随)的硬要求;无需 REPLICA IDENTITY FULL(镜像 upsert 只按主键定位,DEFAULT 即可)
DROP TABLE IF EXISTS order_items, orders, customers;

CREATE TABLE customers (
    id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    city text NOT NULL,
    vip  boolean DEFAULT false
);
COMMENT ON TABLE customers IS '客户';
COMMENT ON COLUMN customers.vip IS '是否 VIP';

CREATE TABLE orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL,
    order_date  date NOT NULL,
    status      text NOT NULL DEFAULT 'paid'  -- paid / shipped / cancelled
);
COMMENT ON TABLE orders IS '订单';

CREATE TABLE order_items (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL,
    product  text NOT NULL,
    qty      int NOT NULL,
    price    numeric(10,2) NOT NULL
);
COMMENT ON TABLE order_items IS '订单明细';

-- 8 个客户(4 城市,3 个 VIP)
INSERT INTO customers (name, city, vip) VALUES
  ('张三', '北京', true),  ('李四', '上海', false),
  ('王五', '北京', false), ('赵六', '深圳', true),
  ('钱七', '上海', false), ('孙八', '广州', false),
  ('周九', '深圳', true),  ('吴十', '广州', false);

-- 24 个订单:近 3 个月分布,约 1/6 取消
INSERT INTO orders (customer_id, order_date, status)
SELECT (g % 8) + 1,
       CURRENT_DATE - (g * 4),
       CASE WHEN g % 6 = 0 THEN 'cancelled'
            WHEN g % 3 = 0 THEN 'shipped'
            ELSE 'paid' END
FROM generate_series(0, 23) g;

-- 48 条明细:每订单 2 条,6 种商品
INSERT INTO order_items (order_id, product, qty, price)
SELECT (g % 24) + 1,
       (ARRAY['机械键盘','显示器','鼠标','USB 集线器','摄像头','降噪耳机'])[(g % 6) + 1],
       (g % 3) + 1,
       (ARRAY[399.00, 1899.00, 129.00, 89.00, 259.00, 899.00])[(g % 6) + 1]
FROM generate_series(0, 47) g;

-- 确认灌入量
SELECT 'customers=' || (SELECT count(*) FROM customers)
    || ' orders=' || (SELECT count(*) FROM orders)
    || ' items=' || (SELECT count(*) FROM order_items) AS loaded;


-- ████████████████████████████████████████████████████████████████████
-- 【第二段】在 DuckDB console 执行(先跑挂载三件套,湖已含上面三张表的镜像)
-- ████████████████████████████████████████████████████████████████████

-- ⓪ 挂载(每次新开会话执行一次;驱动需 DuckDB ≥1.3)
INSTALL ducklake; LOAD ducklake;
INSTALL httpfs;   LOAD httpfs;
CREATE OR REPLACE SECRET rfs (
  TYPE s3, KEY_ID 'admin', SECRET 'changeme',
  ENDPOINT '36.212.12.179:19000', URL_STYLE 'path', USE_SSL false
);
ATTACH IF NOT EXISTS 'ducklake:postgres:host=36.212.12.179 port=15434 dbname=ducklake_catalog user=lake_admin password=changeme'
  AS lake (READ_ONLY);

-- 镜像就位确认(应看到 customers/orders/order_items 各 8/24/48 行)
SELECT 'customers' AS t, count(*) AS rows FROM lake.public.customers
UNION ALL SELECT 'orders', count(*) FROM lake.public.orders
UNION ALL SELECT 'order_items', count(*) FROM lake.public.order_items;

-- ① 建视图(视图存本地 DuckDB 的 main catalog,引用湖表——不写湖、不违反单写者;
--    identifier.db 文件持久保存,重开会话跑完 ⓪ 挂载后视图直接可用)

-- 视图 1:订单明细打平(三表 JOIN,一行 = 明细 + 订单 + 客户完整上下文)
CREATE OR REPLACE VIEW v_order_detail AS
SELECT o.id AS order_id, o.order_date, o.status,
       c.name AS customer, c.city, c.vip,
       i.product, i.qty, i.price, i.qty * i.price AS amount
FROM lake.public.orders o
JOIN lake.public.customers   c ON c.id = o.customer_id
JOIN lake.public.order_items i ON i.order_id = o.id;

-- 视图 2:客户消费画像(视图叠视图——基于视图 1 再聚合)
CREATE OR REPLACE VIEW v_customer_summary AS
SELECT customer, city, vip,
       count(DISTINCT order_id)  AS order_cnt,
       sum(amount)               AS total_spent,
       round(avg(amount), 2)     AS avg_item_amount,
       max(order_date)           AS last_order
FROM v_order_detail
WHERE status <> 'cancelled'
GROUP BY customer, city, vip;

-- 视图 3:月度城市销售报表
CREATE OR REPLACE VIEW v_monthly_city_sales AS
SELECT date_trunc('month', order_date) AS month, city,
       sum(amount)                     AS revenue,
       count(DISTINCT order_id)        AS orders
FROM v_order_detail
WHERE status <> 'cancelled'
GROUP BY 1, 2;

-- ② 演示对比 ──────────────────────────────────────────────

-- 【不用视图】三表 JOIN + 过滤 + 聚合 + 排序,每次写全一遍(还容易漏 cancelled 过滤):
SELECT c.name, c.city, sum(i.qty * i.price) AS total
FROM lake.public.orders o
JOIN lake.public.customers   c ON c.id = o.customer_id
JOIN lake.public.order_items i ON i.order_id = o.id
WHERE o.status <> 'cancelled'
GROUP BY c.name, c.city
ORDER BY total DESC;

-- 【用视图】一行到位:
SELECT * FROM v_customer_summary ORDER BY total_spent DESC;

-- 更多一行查询:
SELECT * FROM v_customer_summary WHERE vip AND total_spent > 2000;      -- 高价值 VIP
SELECT * FROM v_monthly_city_sales ORDER BY month DESC, revenue DESC;   -- 月度报表
SELECT product, sum(qty) AS sold, sum(amount) AS revenue                -- 商品销量榜
FROM v_order_detail WHERE status <> 'cancelled'
GROUP BY product ORDER BY revenue DESC;

-- ③ 实时性压轴:回主库 console 改点数据,比如把 1 号订单取消——
--      UPDATE orders SET status = 'cancelled' WHERE id = 1;
--    回到这里重跑 ② 的任意查询:结果秒级变化。
--    视图固化复杂查询 + CDC 当前态镜像 = 报表永远查的是主库的"现在"。

-- ④ 彩蛋:时间旅行(改完数据后,还能查"改之前"的样子;默认保留 30 天)
-- SELECT * FROM lake.public.orders AT (TIMESTAMP '2026-07-14 10:00:00+08') WHERE id = 1;
