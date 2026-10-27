from shop.db import run_query


def get_order(pool, order_id):
    return run_query(pool, f"SELECT * FROM orders WHERE id = {order_id}")


async def fetch_orders_async(pool, ids):
    return [get_order(pool, i) for i in ids]


class OrderService:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def normalize_id(raw):
        return int(str(raw).strip())

    def cancel(self, order_id):
        return run_query(
            self.pool, f"UPDATE orders SET status='cancelled' WHERE id = {order_id}"
        )