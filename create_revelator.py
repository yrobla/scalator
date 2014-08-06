import skiff
from skiff.Image import SkiffImage
import requests
import sys

def get_images():

    images = [];
    page = 1;

    while True:
    
        r = requests.get(skiff.utils.DO_BASE_URL + '/images?page='+str(page), headers=skiff.utils.DO_HEADERS)
        r = r.json()
        
        if 'message' in r:
            raise ValueError(r['message'])
        else:
            [images.append(SkiffImage(d)) for d in r["images"]]

        if len(images) == int(r['meta']['total']):
            return images;
        else:
            page += 1;
    
    return images;

def create_revelator(name,size,version):

    #Authenticate
    skiff.token('3461da98023e93f0a40e1058092314fbbd5cf057ceb1f1ae1e4d2d6d0d6e2209');

    #Try to find the image ID for the version specified
    image_id = None;
    for image in get_images():
        if image.name == 'Revelator v'+str(version):
            image_id = image.id;
            break;

    #Create the droplet
    skiff.Droplet.create(name='rev.'+name,region='ams2',size=size,image=image_id,private_networking=True,ssh_keys=skiff.Key.all());

if __name__ == '__main__':

    try:
        name = sys.argv[1];
        size = sys.argv[2];
    except IndexError:
        print('python create_revelator.py <name> <size> <snapshot_version>');
        quit();

    try:
        version = sys.argv[3];
    except:
        version = '10';
 
    create_revelator(name,size,version);
