#!/usr/bin/env python3
"""
Classify Alcon products as drugs or devices based on research
"""

import pandas as pd
import os

# Define the product classifications based on research
product_classifications = {
    # Surgical Equipment and Systems (DEVICES)
    "Accurus Vit Equipment": "Device",
    "Constellation": "Device",
    "SMART Suite": "Device",
    "CENTURION": "Device",
    "Centurion": "Device",
    "LenSx": "Device",
    "Infiniti": "Device",
    "Wavelight Refractive Suite": "Device",
    "WaveLight EX500 Excimer Laser": "Device",
    "Wavelight": "Device",
    "Accurus, Alcon": "Device",
    "Accurus, 25+, Alcon": "Device",
    "Luxor": "Device",
    "LuxOR Revalia Ophthalmic Microscope": "Device",
    
    # Visualization and Diagnostic Systems (DEVICES)
    "NGENUITY": "Device",
    "Digital Health Suite": "Device",
    "Verion": "Device",
    "ORA System VerifEye": "Device",
    "ORA": "Device",
    "ARGOS": "Device",
    "Argos 1.5 biometer": "Device",
    "Capella Aberrometer": "Device",
    "Capella Topographer": "Device",
    
    # Intraocular Lenses (IOLs) - All are DEVICES
    "ReSTOR": "Device",
    "Clareon Autonome IOL Injector": "Device",
    "AcrySof UltraSert": "Device",
    "UltraSert": "Device",
    "AcrySof IQ PanOptix UV IOL": "Device",
    "AcrySof IQ PanOptix": "Device",
    "ACTIVEFOCUS": "Device",
    "AcrySof IQ VIVITY": "Device",
    "AcrySof IQ VIVITY IOL": "Device",
    "Acrysof Cachet": "Device",
    "Acrysof, IQ ReSTOR": "Device",
    "ACRYSOF, IQ RESTOR": "Device",
    "Clareon IOL Model C1000U": "Device",
    "AcrySof IQ TORIC IOL": "Device",
    "DT1 UV HEVL Toric": "Device",
    "Acrysof IQ ReSTOR IOL": "Device",
    "AcrySof Toric Aspheric UV Absorbing IOL": "Device",
    "AcrySof": "Device",
    "Clareon": "Device",
    
    # Contact Lenses (DEVICES)
    "TOTAL30": "Device",
    "Air Optix plus HydraGlyde": "Device",
    "DAILIES Multifocal": "Device",
    "DAILIES": "Device",
    "AIR OPTIX": "Device",
    "AIR OPTIX Aqua Multifocal": "Device",
    "Air Optix Color": "Device",
    "DAILIES TOTAL1": "Device",
    "Precision 1": "Device",
    "Precision 7": "Device",
    "Freshlook": "Device",
    "Dailies Aqua Comfort Plus": "Device",
    "Dailies Color": "Device",
    "DAILIES TOTAL30": "Device",
    "AIR OPTIX Aqua": "Device",
    "DAILIES TOTAL1 Multifocal": "Device",
    
    # Ophthalmic Viscosurgical Devices (OVDs) - DEVICES
    "Discovisc": "Device",
    "Duovisc": "Device",
    "Viscoat": "Device",
    
    # Surgical Instruments (DEVICES)
    "FINESSE": "Device",
    "FINESSE SHARKSKIN": "Device",
    "Alcon Greishaber Finesse": "Device",
    "Alcon Closure Systems": "Device",
    "A.C.S (Alcon Closure System)": "Device",
    
    # MIGS Devices (DEVICES)
    "CyPass": "Device",
    "HYDRUS Microstent": "Device",
    
    # MGD Treatment Device (DEVICE)
    "ILUX": "Device",
    "Blephex": "Device",
    
    # Contact Lens Solutions (DEVICES - FDA regulated as medical devices)
    "Opti-Free Express Contact Lens Solution": "Device",
    "Clear Care": "Device",
    "Opti-Free": "Device",
    "Opti-Free Express": "Device",
    
    # Prescription Eye Drops (DRUGS)
    "EYSUVIS": "Drug",
    "Rocklatan": "Drug",
    "rocklatan": "Drug",
    "Azopt": "Drug",
    "Simbrinza": "Drug",
    "rhopressa": "Drug",
    "Rhopressa": "Drug",
    "Pataday": "Drug",
    
    # OTC/Lubricant Eye Drops (DRUGS/OTC)
    "Systane": "Drug",
    "Systane Complete": "Drug",
    
    # Special case - MARLO (appears to be unrelated/unclear)
    "MARLO": "Unknown"
}

def classify_products(input_file, output_file):
    """
    Read the CSV file and add product classification column
    """
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Get the column name (it's the first and only column)
    col_name = df.columns[0]
    
    # Add classification column
    df['product_type'] = df[col_name].map(product_classifications)
    
    # Fill any unmapped values as "Unknown" 
    df['product_type'] = df['product_type'].fillna('Unknown')
    
    # Add a subcategory column for more detail
    df['product_subcategory'] = df[col_name].apply(lambda x: classify_subcategory(x, product_classifications.get(x, 'Unknown')))
    
    # Save the updated CSV
    df.to_csv(output_file, index=False)
    
    # Print summary statistics
    print("Classification Complete!")
    print("\nSummary:")
    print(df['product_type'].value_counts())
    print("\nSubcategories:")
    print(df['product_subcategory'].value_counts())
    
    # Show any unknown products
    unknown_products = df[df['product_type'] == 'Unknown'][col_name].tolist()
    if unknown_products:
        print("\nProducts that couldn't be classified:")
        for product in unknown_products:
            if product and str(product).strip():  # Check for non-empty values
                print(f"  - {product}")
    
    return df

def classify_subcategory(product_name, product_type):
    """
    Classify products into subcategories for more detail
    """
    if pd.isna(product_name) or not str(product_name).strip():
        return "Empty"
    
    product_name = str(product_name).upper()
    
    if product_type == "Device":
        if any(term in product_name for term in ["IOL", "ACRYSOF", "CLAREON", "RESTOR", "PANOPTIX", "VIVITY", "TORIC", "DT1"]):
            return "Intraocular Lens (IOL)"
        elif any(term in product_name for term in ["DAILIES", "AIR OPTIX", "TOTAL30", "PRECISION", "FRESHLOOK"]):
            return "Contact Lens"
        elif any(term in product_name for term in ["CENTURION", "CONSTELLATION", "ACCURUS", "INFINITI", "LENSX"]):
            return "Surgical System"
        elif any(term in product_name for term in ["NGENUITY", "VERION", "ORA", "ARGOS", "CAPELLA"]):
            return "Diagnostic/Visualization"
        elif any(term in product_name for term in ["WAVELIGHT", "LASER"]):
            return "Laser System"
        elif any(term in product_name for term in ["DISCOVISC", "DUOVISC", "VISCOAT"]):
            return "OVD"
        elif any(term in product_name for term in ["CYPASS", "HYDRUS"]):
            return "MIGS Device"
        elif any(term in product_name for term in ["OPTI-FREE", "CLEAR CARE"]):
            return "Contact Lens Solution"
        elif any(term in product_name for term in ["LUXOR", "MICROSCOPE"]):
            return "Microscope"
        elif any(term in product_name for term in ["ILUX", "BLEPHEX"]):
            return "MGD Treatment Device"
        else:
            return "Surgical Instrument/Other"
    
    elif product_type == "Drug":
        if any(term in product_name for term in ["ROCKLATAN", "RHOPRESSA", "AZOPT", "SIMBRINZA"]):
            return "Glaucoma Medication"
        elif "EYSUVIS" in product_name:
            return "Dry Eye Medication"
        elif "PATADAY" in product_name:
            return "Allergy Medication"
        elif "SYSTANE" in product_name:
            return "Lubricant Eye Drop"
        else:
            return "Other Medication"
    
    return "Other"

if __name__ == "__main__":
    # Define file paths
    base_dir = "/home/incent/conflixis-data-projects/projects/189-alcon-custom-report"
    input_file = os.path.join(base_dir, "data/inputs/bquxjob_3469c40f_1993422a87d.csv")
    output_file = os.path.join(base_dir, "data/outputs/alcon_products_classified.csv")
    
    # Ensure output directory exists
    os.makedirs(os.path.join(base_dir, "data/outputs"), exist_ok=True)
    
    # Run classification
    df = classify_products(input_file, output_file)
    
    print(f"\nClassified data saved to: {output_file}")