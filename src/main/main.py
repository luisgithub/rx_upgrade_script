
from src.main.utils.logger_config import setup_logger
from src.main.rx_service import fetch_rx_recordds
from src.main.rx_service import close


logger = setup_logger("erposm - ")

# Example Usage (replace with your actual database credentials)
def main():
    page_size = 100
    offset = 0
    while True:
        records = fetch_rx_recordds(page_size, offset)
        if records:
            logger.info(f"{len(records)} records fetched from DB..")
            for record in records:
                update_field(record)
            offset += page_size
        else:
            logger.info("No records found in query..")
            break
    #close database
    close()
   

def update_field(record):
    lente_desc = record.get("lente_descripcion")
    tipo_lente_desc = record.get("tipo_lente_descripcion")
    if not lente_desc.isdigit() and not tipo_lente_desc.isdigit():
        logger.info(f"lente: {lente_desc}")
    


if __name__ == "__main__":
    main()