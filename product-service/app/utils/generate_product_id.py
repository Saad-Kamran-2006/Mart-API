import random
import string

def generate_product_id() -> str:
        product_id = string.ascii_letters + string.digits
        return ''.join(random.choices(product_id, k=12))