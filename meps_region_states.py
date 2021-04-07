import pandas as pd

#Source: https://www.meps.ahrq.gov/survey_comp/hc_technical_notes.shtml
MEPS_region_states = pd.DataFrame({'Region_Value': [1, 2, 3, 4], 
              'Region_Label': ['Northeast', 'Midwest', 'South', 'West'],
              'States': [['Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'New Jersey',
                        'New York', 'Pennsylvania', 'Rhode Island', 'Vermont'],
                        ['Indiana', 'Illinois', 'Iowa', 'Kansas', 'Michigan', 'Minnesota', 'Missouri',
                        'Nebraska', 'North Dakota', 'Ohio', 'South Dakota', 'Wisconsin'],
                        ['Alabama', 'Arkansas', 'Delaware', 'District of Columbia', 'Florida',
                        'Georgia', 'Kentucky', 'Louisiana', 'Maryland', 'Mississippi', 'North Carolina', 'Oklahoma', 'South Carolina', 'Tennessee', 'Texas', 'Virginia',
                        'West Virginia'],
                        ['Alaska', 'Arizona', 'California', 'Colorado', 'Hawaii', 'Idaho', 'Montana',
                        'Nevada', 'New Mexico', 'Oregon', 'Utah', 'Washington', 'Wyoming']]
                }).set_index(['Region_Value'])['States'].apply(pd.Series).stack().reset_index(level=1, drop=True).reset_index().rename(columns={0:'States'})