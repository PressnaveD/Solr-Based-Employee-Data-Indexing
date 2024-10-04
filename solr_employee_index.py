import pysolr
import pandas as pd

SOLR_URL = 'http://localhost:8983/solr/'


solr_pressnave = pysolr.Solr(SOLR_URL + 'Pressnave', always_commit=True)
solr_3545 = pysolr.Solr(SOLR_URL + '3545', always_commit=True)

def indexData(solr, p_exclude_column):
    """Index data into the specified collection after excluding a column."""
    try:
       
        df = pd.read_csv(r'D:\aadrive python\employee_sample_data.csv', encoding='ISO-8859-1', on_bad_lines='warn')
        
        if p_exclude_column in df.columns:
            df = df.drop(columns=[p_exclude_column])
    
        data = df.to_dict(orient='records')
        solr.add(data) 
        print(f"Data indexed excluding column '{p_exclude_column}'.")
    except Exception as e:
        print(f"Error indexing data: {e}")

def searchByColumn(solr, p_column_name, p_column_value):
    """Search for a specific value in a given column of a collection."""
    try:
        results = solr.search(f'{p_column_name}:{p_column_value}') 
        return list(results)
    except Exception as e:
        print(f"Error searching in collection: {e}")
        return []

def getEmpCount(solr):
    """Get the total count of employees in a collection."""
    try:
        response = solr.search('*:*')
        return len(response)
    except Exception as e:
        print(f"Error getting employee count: {e}")
        return 0

def delEmpById(solr, p_employee_id):
    """Delete an employee from a collection by ID."""
    try:
        solr.delete(id=p_employee_id) 
        print(f"Employee with ID '{p_employee_id}' deleted.")
    except Exception as e:
        print(f"Error deleting employee with ID '{p_employee_id}': {e}")

def getDepFacet(solr):
    """Get department facets from a collection."""
    try:
        results = solr.search('*:*', **{'facet': 'true', 'facet.field': 'Department'})
        return results.facets
    except Exception as e:
        print(f"Error getting department facets: {e}")
        return {}


if __name__ == "__main__":
 
    print("Employee count in Pressnave collection:", getEmpCount(solr_pressnave))
    print("Employee count in 3545 collection:", getEmpCount(solr_3545))


    print("Indexing data into Pressnave...")
    indexData(solr_pressnave, 'Department') 
    print("Indexing data into 3545...")
    indexData(solr_3545, 'Gender')            

    
    print("Updated employee count in Pressnave collection:", getEmpCount(solr_pressnave))
    print("Updated employee count in 3545 collection:", getEmpCount(solr_3545))

    employee_id_to_delete = 'E02003' 
    print(f"Deleting employee with ID '{employee_id_to_delete}' from Pressnave collection...")
    delEmpById(solr_pressnave, employee_id_to_delete)

  
    print("Searching for employees in IT Department in Pressnave collection...")
    print(searchByColumn(solr_pressnave, 'Department', 'IT'))

    print("Searching for Male employees in Pressnave collection...")
    print(searchByColumn(solr_pressnave, 'Gender', 'Male'))

    print("Searching for IT Department in 3545 collection...")
    print(searchByColumn(solr_3545, 'Department', 'IT'))

   
    print("Getting department facets for Pressnave collection...")
    print(getDepFacet(solr_pressnave))

    print("Getting department facets for 3545 collection...")
    print(getDepFacet(solr_3545))
