from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def catalogue_sections_getter():
    #bs4 & requests
    webpage = requests.get('https://eyewa.com/ae-en/', 'html.parser')
    soup = BeautifulSoup(webpage.content)

    #fina\d the header
    header_menu = soup.find(attrs={"id": "mainMenu"}).find_all('li', attrs={"class":["mega-menu-item mega-menu-fullwidth menu-3columns level0 static-menu level0 dropdown","mega-menu-item mega-menu-fullwidth menu-2columns level0 static-menu level0 dropdown"]} )

    #input the section into a dict
    sections_dict = {}
    for section in header_menu:
        #print(section.a.span.string, section.a['href'])
        sections_dict[section.a.span.string] = section.a['href']

    return sections_dict

def product_list_getter(sections_dict):
    all_products_dict = {}
    for x,y in sections_dict.items():
        next_link=y
        while next_link != True:
            product_details_getter(all_products_dict, next_link)
            next_link = next_page_finder(next_link)
    
    return all_products_dict


def product_details_getter(all_products_dict, link):
    print(f'getting data from {link}')
    webpage = requests.get(link, 'html.parser')
    soup = BeautifulSoup(webpage.content)
    try:
        product_list =  soup.find(attrs={'class':"products list items product-items row row-col-lg-3"})
        for i in product_list.find_all('li'): 
            product_top =i.find(attrs={'class':'product-top'})
            product_url = product_top.a['href']
            product_top = product_top.a.find('img')
            product_name = product_top['alt']
            product_picture = product_top['src']
            
            all_products_dict[product_name]={}
            all_products_dict[product_name]['product_url']=product_url
            all_products_dict[product_name]['picture_url']=product_picture
    except Exception as e:
        print(e)

def product_detail_getter_2(all_products_dict):
    
    for key in all_products_dict.keys():
        current_link = all_products_dict[key]['product_url']
        links_not_working = {}

        print(f'getting data from {current_link}')
        html = requests.get(current_link)
        
        if html.status_code == 404:
            links_not_working[key] = current_link
            print(f'html response for {key}: {html.status_code}')
            continue
        else:
            print(f'html response for {key}: {html.status_code}')
        
        
        soup = BeautifulSoup(html.content)

        retries=0
        if  soup.find(attrs={'class':'cf-error-type'}) != None :
            if retries < 5:
                print(soup.find(attrs={'class':'cf-error-type'}))
                print(f'retrying a new connection to {current_link}')
                html = requests.get(current_link)
                soup = BeautifulSoup(html.content)
                retries += 1
        
            else:
                print(f"Current link didn't work: {current_link}")
                links_not_working[key] = current_link
                continue

        
        try:
            id = soup.find(attrs={'class':'price-box price-final_price'})['data-product-id']
            all_products_dict[key]['product_id'] = id
        except:
            print(f"couldnt find product id for {key}")
            print(soup.find(attrs={'class':'price-box price-final_price'}))

        retries = 0    
        if soup.find(attrs={'id':'productPriceInfo'}) is None:
            if retries < 5:
                print(f'retrying a new connection to {current_link}')
                html = requests.get(current_link)
                soup = BeautifulSoup(html.content)
                retries += 1
            else:
                continue
        else:
            price =  soup.find(attrs={'id':'productPriceInfo'})
            print(f'found productPriceInfo tag for {key}')
            try:
                oldPrice = price.find(attrs={'class':'price-wrapper','data-price-type':'oldPrice'})['data-price-amount']
                currentPrice = price.find(attrs={'class':'price-wrapper','data-price-type':'finalPrice'})['data-price-amount']
                all_products_dict[key]['discounted'] = 'yes'
                all_products_dict[key]['discountedPrice'] = currentPrice
                all_products_dict[key]['actualPrice'] = oldPrice
                

            except Exception as e:
                print(e)
                print(price)
                currentPrice = price.find(attrs={'class':'price-wrapper','data-price-type':'finalPrice'})['data-price-amount']   
                all_products_dict[key]['discounted'] = 'no'
                all_products_dict[key]['actualPrice'] = currentPrice



        table = soup.find(attrs={'class':'data table additional-attributes'}).find('tbody')
        rows = table.find_all('td')
        print(f'Getting product attributes for {key}')
        for row in range(len(rows)):        
            all_products_dict[key][rows[row]['data-th']] = rows[row].string
    

    return all_products_dict

def next_page_finder(current_link):
    try:
        next_link = requests.get(current_link, 'html.parser')
        next_link = BeautifulSoup(next_link.content)
        next_link = next_link.find('li',attrs={'class':'item pages-item-next'}).a['href']
        return next_link

    except Exception as e:
        print('End of the ride for this category')
        return True

def product_dataframe(all_products_dict):
    df = pd.DataFrame.from_dict(all_products_dict,orient='index')
    print(df.head())
    return df



if __name__ == '__main__':
    sections_dict = catalogue_sections_getter()
    all_products_dict = product_list_getter(sections_dict)
    all_products_dict2 = product_detail_getter_2(all_products_dict)
    df= pd.DataFrame.from_dict(all_products_dict2,orient='index')
    df['date_created'] = datetime.now()
    df.astype({'actualPrice': 'float', 'discountedPrice': 'float'})
    engine = create_engine('sqlite:///../data/eyewa.db', echo=False)
    df.to_sql('eyewa_catalogue_current', con=engine, if_exists='replace')
    df.to_sql('eyewa_catalogue_archive', con=engine, if_exists='append')