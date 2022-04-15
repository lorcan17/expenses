from splitwise import Splitwise
import pandas as pd

def GetCategoryIDs(Splitwise):
    """Create a Dataframe with all Splitwise Categories, with IDs"""
    rows = []
    
    cat = Splitwise.getCategories()
    for x in cat:
      subcat =  x.getSubcategories()
      for y in subcat:
        rows.append([x.getName(), y.getName(),y.getId()])    
      
    cat_df = pd.DataFrame(rows, columns=["Main Category", "SubCategory","CatID"])
    
    cat_df['Category'] = cat_df['Main Category'] + ': ' +  cat_df['SubCategory']
    
    return cat_df