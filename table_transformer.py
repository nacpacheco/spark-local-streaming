import abc

from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, concat, col


class BaseTransformer(abc.ABC):
    """
     Base class for transforming event
     into multiple tables format
    """

    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Abstract method where the logic to transform the event resides.
        It would be implemented by the different table format classes
        :param df: DataFrame
        :return: DataFrame
        """
        raise NotImplementedError


class CustomerTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the Customer table format
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns_to_return = ["id", "name", "email", "phone_area_code", "phone_country_code", "phone_number", "document",
                             "type"]
        return df \
            .withColumn("id", df.objt_clie.cod_rfrc_clie) \
            .withColumn("name", df.objt_clie.nom_clie) \
            .withColumn("email", df.objt_clie.nom_ende_emai_clie) \
            .withColumn("phone_area_code", df.objt_clie.objt_tel_clie.cod_area_tel) \
            .withColumn("phone_country_code", df.objt_clie.objt_tel_clie.cod_pais_tel) \
            .withColumn("phone_number", df.objt_clie.objt_tel_clie.num_tel_clie) \
            .withColumn("document", df.objt_clie.nom_tipo_docm) \
            .withColumn("type", df.objt_clie.cod_tipo_pess) \
            .select(*columns_to_return)


class DeliveryAddressTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the DeliveryAddress table format
    """

    def transform(self, df: DataFrame) -> (DataFrame, DataFrame):
        columns_to_return = ["id", "receiver_name", "street", "city", "short_state", "state", "number",
                             "zip_code", "country_code", "complement", "neighborhood"]
        result_df = df \
            .withColumn("id", monotonically_increasing_id()) \
            .withColumn("receiver_name", df.objt_ende_entg_pedi.nom_pess) \
            .withColumn("street", df.objt_ende_entg_pedi.nom_logr_ende_entg) \
            .withColumn("city", df.objt_ende_entg_pedi.nom_cida_ende_entg) \
            .withColumn("short_state", df.objt_ende_entg_pedi.sig_uf) \
            .withColumn("state", df.objt_ende_entg_pedi.nom_estd) \
            .withColumn("number", df.objt_ende_entg_pedi.num_logr_ende_entg) \
            .withColumn("zip_code", df.objt_ende_entg_pedi.cod_cep) \
            .withColumn("country_code", df.objt_ende_entg_pedi.cod_pais_resd) \
            .withColumn("complement", df.objt_ende_entg_pedi.nom_cmpl_ende_entg) \
            .withColumn("neighborhood", df.objt_ende_entg_pedi.nom_bair_ende_entg)

        return result_df.select(*columns_to_return), result_df.select(col("id").alias("delivery_address_id"),
                                                                      "txt_detl_idt_pedi_pgto")


class SellerTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the Seller table format
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns_to_return = ["id", "sku_id", "name"]

        return df \
            .withColumn("id", df.idt_venr) \
            .withColumn("sku_id", df.idt_vrne_venr) \
            .withColumn("name", df.nom_venr) \
            .select(*columns_to_return) \
            .dropDuplicates(columns_to_return)


class ItemTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the Item table format
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns_to_return = ["id", "product_name", "product_slug", "name", "thumbnail", "warehouse_id", "seller_id",
                             "price", "original_price", "unit_price", "unit_original_price", "price_discount",
                             "unit_financial_discount", "unit_reversl_value"]

        return df \
            .withColumn("id", df.cod_vrne_prod) \
            .withColumn("product_name", df.nom_prod_orig) \
            .withColumn("product_slug", df.txt_plae_otmz_url_prod) \
            .withColumn("name", df.nom_item) \
            .withColumn("thumbnail", df.nom_url_img_oril) \
            .withColumn("warehouse_id", df.list_cod_loja_vend.cod_loja_vend) \
            .withColumn("seller_id", df.idt_venr) \
            .withColumn("price", df.vlr_prod) \
            .withColumn("original_price", df.vlr_oril) \
            .withColumn("unit_price", df.vlr_prec_unit_brut) \
            .withColumn("unit_original_price", df.vlr_oril_prod_unit_vrne) \
            .withColumn("price_discount", df.vlr_prod_ofrt_desc) \
            .withColumn("unit_financial_discount", df.vlr_desc_finn_unit_item) \
            .withColumn("unit_reversl_value", df.vlr_fina_esto_item) \
            .select(*columns_to_return)


class PromotionTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the Promotion table format
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns_to_return = ["id", "name", "discount_type", "coupon_id", "coupon_type", "coupon_code", "value",
                             "value_applied"]

        return df \
            .withColumn("id", df.cod_prmo) \
            .withColumn("name", df.nom_prmo) \
            .withColumn("discount_type", df.nom_tipo_desc) \
            .withColumn("coupon_id", df.objt_cupo.cod_idef_cupo) \
            .withColumn("coupon_type", df.objt_cupo.nom_tipo_cupo) \
            .withColumn("coupon_code", df.objt_cupo.cod_cupo) \
            .withColumn("value", df.vlr_cnfg_desc) \
            .withColumn("value_applied", df.vlr_aplc_desc) \
            .select(*columns_to_return)


class ShipmentTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the Shipment table format
    """

    def transform(self, df: DataFrame) -> (DataFrame, DataFrame):
        columns_to_return = ["id", "substatus_code", "substatus_reason", "status", "seller_id", "billing_number",
                             "billing_serial", "billing_xml_content",
                             "billing_issue_date", "tracking_send_date", "tracking_carrier", "tracking_delivery_method",
                             "delivered_date"]

        return df \
            .withColumn("id", monotonically_increasing_id()) \
            .withColumn("substatus_code", df.objt_sub_stat_envo.cod_sub_stat_entg) \
            .withColumn("substatus_reason", df.objt_sub_stat_envo.nom_sub_stat_entg) \
            .withColumn("status", df.nom_stat_entg) \
            .withColumn("seller_id", df.cod_venr) \
            .withColumn("billing_number", df.objt_carg_fatm.cod_nota_fisc) \
            .withColumn("billing_serial", df.objt_carg_fatm.cod_srie) \
            .withColumn("billing_xml_content", df.objt_carg_fatm.txt_xml_oper) \
            .withColumn("billing_issue_date", df.objt_carg_fatm.dat_emis_nail) \
            .withColumn("tracking_send_date", df.objt_des_stat_rtmt.dat_envo_rtmt) \
            .withColumn("tracking_protocol", df.objt_des_stat_rtmt.prco_rtmt) \
            .withColumn("tracking_carrier", df.objt_des_stat_rtmt.nom_trtd) \
            .withColumn("tracking_delivery_method", df.objt_des_stat_rtmt.des_moda_entg) \
            .withColumn("delivered_date", df.dat_emis_cpvt)\
            .select(*columns_to_return)


class ItemShipmentTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the ItemShipment table format
    """

    def transform(self, df: DataFrame) -> (DataFrame, DataFrame):
        columns_to_return = ["id", "item_id", "shipment_id", "seller_id", "substatus_code", "substatus_reason",
                             "created_at", "updated_at", "quantity", "status"]
        return df \
            .withColumn("id", monotonically_increasing_id()) \
            .withColumn("item_id", df.cod_prod) \
            .withColumn("shipment_id", monotonically_increasing_id()) \
            .withColumn("seller_id", df.cod_prod_venr) \
            .withColumn("substatus_code", df.objt_sub_stat_item.cod_sub_stat_item) \
            .withColumn("substatus_reason", df.objt_sub_stat_item.nom_sub_stat_item) \
            .withColumn("created_at", df.dat_hor_cria_item_entg) \
            .withColumn("updated_at", df.dat_hor_atui_item_entg) \
            .withColumn("quantity", df.qtd_prod) \
            .withColumn("status", df.stat_entg) \
            .select(*columns_to_return)


class FactOrderItemTransformer(BaseTransformer):
    """
    Class to transform raw event 
    into the FactOrderItem table format
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns_to_return = ["id", "order_id", "item_id", "offer_id", "shipping_cost", "shipping_discount",
                             "final_shipping_cost",
                             "estimated_exact_date_iso", "item_quantity", "delivery_method_name",
                             "estimated_business_days",
                             "logistics_provider_name",
                             "store_id", "client_id", "payment_status",
                             "summary_items", "summary_total", "summary_total_price_discount",
                             "summary_total_shipping_cost", "summary_total_shipping_discount",
                             "summary_max_shipping_time",
                             "summary_orders_discount", "summary_final_price", "verify_token",
                             "created_at",
                             "updated_at", "delivery_address_id", "item_shipment_id", "customer_id",
                             "promotion_id"]

        return df \
            .withColumn("id", concat(df.txt_detl_idt_pedi_pgto, df.cod_vrne_prod)) \
            .withColumn("order_id", df.txt_detl_idt_pedi_pgto) \
            .withColumn("item_id", df.cod_vrne_prod) \
            .withColumn("offer_id", df.cod_ofrt_ineo_requ) \
            .withColumn("shipping_cost", df.vlr_fret) \
            .withColumn("shipping_discount", df.vlr_fret) \
            .withColumn("final_shipping_cost", df.vlr_totl_fret) \
            .withColumn("estimated_exact_date_iso", df.dat_hor_esda_entg_item) \
            .withColumn("item_quantity", df.qtd_item_pedi) \
            .withColumn("delivery_method_name", df.des_moda_entg_item) \
            .withColumn("estimated_business_days", df.qtd_dia_prz_entg) \
            .withColumn("logistics_provider_name", df.nom_oped_empr) \
            .withColumn("store_id", df.idt_loja) \
            .withColumn("client_id", df.cod_idef_clie) \
            .withColumn("payment_status", df.stat_pgto) \
            .withColumn("summary_items", df.objt_des_rsum.num_item) \
            .withColumn("summary_total", df.objt_des_rsum.vlr_totl_pedi) \
            .withColumn("summary_total_price_discount", df.objt_des_rsum.vlr_totl_desc) \
            .withColumn("summary_total_shipping_cost", df.objt_des_rsum.vlr_cust_totl_envo) \
            .withColumn("summary_total_shipping_discount", df.objt_des_rsum.vlr_desc_totl_envo) \
            .withColumn("summary_max_shipping_time", df.objt_des_rsum.qtd_maxi_temp_envo) \
            .withColumn("summary_orders_discount", df.objt_des_rsum.vlr_desc_conc_pedi) \
            .withColumn("summary_final_price", df.objt_des_rsum.vlr_fina_pedi) \
            .withColumn("verify_token", df.cod_aces_tokn) \
            .withColumn("created_at", df.dat_hor_tran) \
            .withColumn("updated_at", df.dat_atui) \
            .withColumn("delivery_address_id", df.delivery_address_id) \
            .withColumn("item_shipment_id", monotonically_increasing_id()) \
            .withColumn("customer_id", monotonically_increasing_id()) \
            .withColumn("promotion_id", monotonically_increasing_id()) \
            .select(*columns_to_return)
