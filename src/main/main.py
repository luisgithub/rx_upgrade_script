
from src.main.utils.logger_config import setup_logger
from src.main.rx_service import fetch_rx_records, get_id_tipo_producto_by_description, get_id_producto_by_description_and_clinic,execute_update_query
from src.main.rx_service import close


logger = setup_logger("erposm - ")

# Example Usage (replace with your actual database credentials)
def main():
    page_size = 100
    offset = 0
    updated_rec_counter = 0
    while True:
        records = fetch_rx_records(page_size, offset)
        if records:
            logger.info(f"{len(records)} records fetched from DB..")
            for record in records:
                update_field(record)
            offset += page_size
            updated_rec_counter += len(records)
        else:
            logger.info("No more records found in query..")
            break
    logger.info(f"Total de registros actualizados: {updated_rec_counter}")
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

        if tipo_lente_id ==None or producto_id == None:
            logger.info(f"No se encontro el registro para tipo_lente: {tipo_lente_desc} - lente: {lente_desc} - clinica:{clinica_id}")
        else:
            query_update = f"UPDATE rx SET lente_rx = {producto_id},  tipo_lente_rx = {tipo_lente_id} WHERE id = {rx_id};"
            execute_update_query(query_update)
            logger.info(query_update)
            
    


if __name__ == "__main__":
    main()