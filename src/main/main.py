
from src.main.utils.logger_config import setup_logger
from src.main.rx_service import fetch_rx_records, get_id_tipo_producto_by_description, get_id_producto_by_description_and_clinic
from src.main.rx_service import close


logger = setup_logger("erposm - ")

# Example Usage (replace with your actual database credentials)
def main():
    page_size = 100
    offset = 0
    while True:
        records = fetch_rx_records(page_size, offset)
        if records:
            logger.info(f"{len(records)} records fetched from DB..")
            for record in records:
                logger.info(f"record: {record}")
                update_field(record)
            offset += page_size
        else:
            logger.info("No records found in query..")
            break
    #close database
    close()
   

def update_field(record):
    rx_id = record.get("id")
    clinica_id = record.get("clinica")
    lente_desc = record.get("lente_descripcion")
    tipo_lente_desc = record.get("tipo_lente_descripcion")
    if not lente_desc.isdigit() and not tipo_lente_desc.isdigit():
        tipo_lente_id = get_id_tipo_producto_by_description(tipo_lente_desc)
        
        producto_id = get_id_producto_by_description_and_clinic(lente_desc, clinica_id)
    
        logger.info(f"UPDATE rx SET lente_rx = {producto_id},  tipo_lente_rx = {tipo_lente_id} WHERE id = {rx_id};")
    


if __name__ == "__main__":
    main()