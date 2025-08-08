#!/usr/bin/env python3
import pandas as pd

df = pd.read_csv('data/raw/disclosures/real_disclosures_2025-08-08.csv')

print('Field population rates:')
print(f'  NPIs: {df.provider_npi.notna().sum()}/{len(df)} ({df.provider_npi.notna().sum()*100/len(df):.1f}%)')

email_count = (df.provider_email != '').sum() if 'provider_email' in df.columns else 0
print(f'  Emails: {email_count}/{len(df)} ({email_count*100/len(df):.1f}%)')

manager_count = (df.manager_name != '').sum() if 'manager_name' in df.columns else 0
print(f'  Managers: {manager_count}/{len(df)} ({manager_count*100/len(df):.1f}%)')

dept_count = (df.department != 'Healthcare').sum() if 'department' in df.columns else 0
print(f'  Departments with entity: {dept_count}/{len(df)} ({dept_count*100/len(df):.1f}%)')

specialty_count = (df.specialty != 'Not Specified').sum() if 'specialty' in df.columns else 0
print(f'  Specialties with job title: {specialty_count}/{len(df)} ({specialty_count*100/len(df):.1f}%)')

# Show some sample records with complete info
print('\nSample records with complete member info:')
complete = df[(df.provider_npi.notna()) & (df.provider_email != '') & (df.manager_name != '')]
if len(complete) > 0:
    for idx, row in complete.head(3).iterrows():
        print(f"  - {row.provider_name} (NPI: {row.provider_npi}, Email: {row.provider_email}, Manager: {row.manager_name}, Entity: {row.department})")