import azure.functions as func
import logging
import os
from datetime import datetime, timedelta
from UploadToElastic import run_upload_to_elastic
from DeleteFromElastic import run_delete_for_all_product_areas, single_delete_run

app = func.FunctionApp()

def get_cron_expression():
    """
    Generate NCRONTAB expression based on days or minutes interval.
    Format: {second} {minute} {hour} {day} {month} {day_of_week}
    """
    try:
        days = int(os.getenv('UPLOAD_INTERVAL_DAYS', '7'))
        minutes = int(os.getenv('UPLOAD_INTERVAL_MINUTES', '0'))
        
        # Validation
        if days < 0 or minutes < 0:
            logging.error("Intervals cannot be negative. Using default of 7 days.")
            return "0 0 0 */7 * *"
            
        if days == 0 and minutes == 0:
            logging.error("When days is 0, minutes must be greater than 0. Using default of 7 days.")
            return "0 0 0 */7 * *"
            
        # If days is 0, use minute interval
        if days == 0:
            return f"0 */{minutes} * * * *"  # Run every N minutes
            
        # For daily runs
        if days == 1:
            return "0 0 0 * * *"  # Run at midnight every day
        # For weekly runs
        elif days == 7:
            return "0 0 0 * * 0"  # Run at midnight every Sunday
        # For other day intervals
        else:
            return f"0 0 0 */{days} * *"  # Run at midnight every N days
    except ValueError:
        logging.error("Invalid interval values. Using default of 7 days.")
        return "0 0 0 */7 * *"

def get_time_delta():
    """
    Calculate the time delta based on days or minutes interval
    """
    try:
        days = int(os.environ['UPLOAD_INTERVAL_DAYS'])
        minutes = int(os.environ['UPLOAD_INTERVAL_MINUTES'])
        
        if days == 0:
            return timedelta(minutes=minutes)
        return timedelta(days=days)
    except ValueError:
        return timedelta(days=7)
    

@app.timer_trigger(
    schedule=get_cron_expression(),  # {second} {minute} {hour} {day} {month} {day_of_week}
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def TimerTrigger(myTimer: func.TimerRequest) -> None:
    """
    Timer trigger function that runs based on the interval specified in 
    UPLOAD_INTERVAL_DAYS and UPLOAD_INTERVAL_MINUTES.
    """
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Starting elastic upload process...')
    
    try:
        # Get appropriate time delta based on configuration
        time_delta = get_time_delta()
        last_processed_time = datetime.now() - time_delta
        
        # Call your existing function
        run_upload_to_elastic(
            from_azure_container=True,
            from_directory=False,
            is_sample_questions=False,
            check_for_duplicates_in_elastic=False,
            last_processed_time=last_processed_time,
            prefix=None
        )
        
        logging.info("Upload to Elastic completed successfully")
        
    except Exception as e:
        logging.error(f"Error in timer triggered function: {str(e)}")
        raise

@app.timer_trigger(
    schedule=get_cron_expression(),
    arg_name="deleteTimer",
    run_on_startup=False,
    use_monitor=False
)
def DeleteTimerTrigger(deleteTimer: func.TimerRequest) -> None:
    """
    Timer trigger function that handles deletion of documents from Elasticsearch
    based on the configured interval.
    """
    if deleteTimer.past_due:
        logging.info('The delete timer is past due!')

    logging.info('Starting elastic delete process...')
    
    try:
        # Calculate the time window for deletion
        time_window = get_time_delta()
        last_modified_date = datetime.now() - time_window
        
        # Configure deletion parameters
        hard_delete = os.environ.get('HARD_DELETE', 'False').lower() == 'true'
        run_for_all_products = os.environ.get('RUN_FOR_ALL_PRODUCTS', 'True').lower() == 'true'
        
        if run_for_all_products:
            logging.info(f'Running delete for all product areas with hard_delete={hard_delete}')
            run_delete_for_all_product_areas(
                last_modified_date=last_modified_date,
                hard_delete=hard_delete
            )
        else:
            # Single product deletion
            index_to_delete_from = os.getenv('INDEX_TO_DELETE_FROM')
            
            if not index_to_delete_from:
                raise ValueError("INDEX_TO_DELETE_FROM environment variable is required for single product deletion")
                
            logging.info(f'Running single delete for index {index_to_delete_from}')

            single_delete_run(
                product_area=index_to_delete_from,
                hard_delete=hard_delete,
                last_modified_date=last_modified_date
            )
        
        logging.info("Delete from Elastic completed successfully")
        
    except Exception as e:
        logging.error(f"Error in delete timer triggered function: {str(e)}")
        raise