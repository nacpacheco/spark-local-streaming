TABLES_DDLS = [
    """
    CREATE TABLE IF NOT EXISTS customer(
        id text PRIMARY KEY,
        name text,
        email text,
        phone_area_code text,
        phone_country_code text,
        phone_number text,
        document text,
        type text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS delivery_address(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        receiver_name text,
        street text,
        city text,
        short_state text,
        state text,
        number text,
        zip_code text,
        country_code text,
        complement text,
        neighborhood text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS seller(
        id text PRIMARY KEY,
        sku_id text,
        name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS item(
        id text PRIMARY KEY, --cod_vrne_prod
        product_name text,
        product_slug text,
        name text,
        slug text,
        thumbnail text,
        warehouse_id text NOT NULL,
        seller_id text NOT NULL,
        price double,
        original_price double,
        unit_price double,
        unit_original_price double,
        price_discount double,
        unit_financial_discount text,
        unit_reversl_value text,
        FOREIGN KEY (warehouse_id) references warehouse (id),
        FOREIGN KEY (seller_id) references seller (id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS promotion(
        id text PRIMARY KEY, --cod_prmo
        name text,
        discount_type text,
        coupon_id text,
        coupon_type text,
        coupon_code text,
        value double,
        value_applied double
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shipment(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        substatus_code text,
        substatus_reason text,
        substatus_description text,
        status text,
        seller_id text NOT NULL,
        billing_number text,
        billing_serial text,
        billing_xml_content text,
        billing_issue_date text,
        tracking_send_date text,
        tracking_protocol text,
        tracking_carrier text,
        tracking_delivery_method text,
        delivered_date text,
        FOREIGN KEY (seller_id) references seller (id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS item_shipment(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id text not null, --cod_prod
        shipment_id INTEGER NOT NULL,
        seller_id text, --cod_seller
        substatus_code text,
        substatus_reason text,
        created_at text,
        updated_at text,
        quantity bigint,
        status text,
        FOREIGN KEY (item_id) references item (id),
        FOREIGN KEY (shipment_id) references shipment (id),
        FOREIGN KEY (seller_id) references seller (id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_item(
        id PRIMARY KEY,
        order_id text, --txt_detl_idt_pedi_pgto
        item_id text NOT NULL,
        offer_id text,
        shipping_cost double,
        shipping_discount double,
        final_shipping_cost double,
        estimated_exact_date_iso text,
        item_quantity bigint,
        delivery_method_name text,
        estimated_business_days text,
        logistics_provider_name text,
        store_id text,
        client_id text,
        payment_status text,
        summary_items double,
        summary_total double,
        summary_total_price_discount double,
        summary_total_shipping_discount double,
        summary_max_shipping_time double,
        summary_orders_discount double,
        summary_final_price double,
        verify_token text,
        created_at date,
        updated_at date,
        delivery_address_id integer NOT NULL,
        item_shipment_id integer NOT NULL,
        customer_id text NOT NULL,
        promotion_id text,
        FOREIGN KEY (item_id) references item (id),
        FOREIGN KEY (delivery_address_id) references delivery_address (id),
        FOREIGN KEY (customer_id) references customer (id),
        FOREIGN KEY (promotion_id) references promotion (id),
        FOREIGN KEY (item_shipment_id) references item_shipment(id)
    )
    """,
]
