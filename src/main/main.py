
from src.main.utils.logger_config import setup_logger
from src.main.rx_service import fetch_rx_records, get_id_tipo_producto_by_description, get_id_producto_by_description_and_clinic,execute_update_query, fetch_rx_records_color, get_id_color_by_descripcion
from src.main.rx_service import close


logger = setup_logger("erposm - ")

# Example Usage (replace with your actual database credentials)
def main():
    # process_update_lente_field()
    process_update_color_field()
    close()

def process_update_lente_field():
    page_size = 100
    offset = 0
    updated_rec_counter = 0
    while True:
        records = fetch_rx_records(page_size, offset)
        if records:
            logger.info(f"{len(records)} rx records lente fetched from DB..")
            for record in records:
                update_lente_field(record)
            offset += page_size
        else:
            logger.info("No more lente records found in DB..")
            break
    logger.info(f"Total de registros actualizados: {updated_rec_counter}")

def process_update_color_field():
    page_size = 100
    offset = 0
    updated_rec_counter = 0
    page_counter = 0
    while True:
        records = fetch_rx_records_color(page_size, offset)
        if records:
            logger.info(f"{len(records)} rx color records fetched from DB.." )
            for record in records:
                update_color_field(record)
            offset += page_size
            updated_rec_counter += len(records)
            page_counter += 1
            logger.info(f"Page {page_counter} has been processed..")
        else:
            logger.info(f"No more color records found in DB..")
            break
    logger.info(f"Total amount of color records updated: {updated_rec_counter}")            

def update_lente_field(record):
    rx_id = record.get("id")
    clinica_id = record.get("clinica")
    lente_desc = record.get("lente_descripcion")
    tipo_lente_desc = record.get("tipo_lente_descripcion")
    if not lente_desc.isdigit() and not tipo_lente_desc.isdigit():
        tipo_lente_id = get_id_tipo_producto_by_description(tipo_lente_desc)
        
        producto_id = get_id_producto_by_description_and_clinic(lente_desc, clinica_id)

        if tipo_lente_id == None or producto_id == None:
            logger.info(f"No se encontro el registro para tipo_lente: {tipo_lente_desc} - lente: {lente_desc} - clinica:{clinica_id}")
        else:
            query_update = f"UPDATE rx SET lente_rx = {producto_id},  tipo_lente_rx = {tipo_lente_id} WHERE id = {rx_id};"
            execute_update_query(query_update)
            
def update_color_field(record):
    rx_id = record.get('id')
    current_value = record.get('color')
    
    if current_value != None:
        logger.info(f"looking for color in row: {record}")
        color_id = get_id_color_by_descripcion(current_value)
        query_update = f"UPDATE rx SET color_armazon = {color_id} WHERE id = {rx_id};"
        execute_update_query(query_update)
        # logger.info(query_update)


if __name__ == "__main__":
    main()