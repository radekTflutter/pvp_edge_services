# pvp_edge_services

The PVP Edge Services application is a local, on-premise system designed for the automated verification of pallets on the production line. Its primary function is to read codes from pallet labels and cans, validate their correctness against data retrieved from a central ERP system (SAP), and determine if the physical label is placed on the correct product.

Core Components and Process Flow:
Data Acquisition (SAP API Service)

The sapapi_service_.py continuously polls the central SAP API to fetch the list of expected orders or Handling Units (HUs) based on EAN numbers and pallet codes. This list serves as the single source of truth and is stored locally in the application's database.

Scanning and Signal Monitoring (PLC, Scanner, Reader Services)

The plc_service_main.py monitors signals from the machine's PLC (Programmable Logic Controller), particularly the trigger signal indicating a pallet is in the correct position for scanning (PaletPosition).

The scanner_service_main.py processes the physical scan of the pallet label (the metka code) and records the data (EAN, HU, and a unique ID) into the local database.

Verification and Decision (Validation Service)

The validation_service_main.py is the core decision engine. It retrieves the latest scanned data and compares it with the list of expected/approved records previously downloaded from SAP.

It determines whether the scanned product is Correct (OK) or Incorrect (NOK).

The validation result is then communicated back to the PLC via the PLC service, controlling a conveyor.

Reporting and Archiving (API and Photo API Services)

The api_service_main.py sends the validation results (Pallet ID, EAN, HU, and OK/NOK status) back to the central API to update the pallet's status in the overarching system.

The photo_api_service_main.py manages the upload of inspection images (captured by photoblob_service_main.py upon PLC triggers) to the central API, providing visual evidence of the verification process.

In Summary: The system fetches expected pallet data from SAP, scans the physical label, validates the match, signals the result to the machine (OK/NOK), and reports the status and associated photos to the central system.
