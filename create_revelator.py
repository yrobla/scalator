import skiff
from skiff.Image import SkiffImage
import requests
import sys

def create_revelator(name,size,version):

    #Authenticate
    s = skiff.rig('3461da98023e93f0a40e1058092314fbbd5cf057ceb1f1ae1e4d2d6d0d6e2209');

    #Try to find the image ID for the version specified
    images= s.Image.all();
    for image in images:
        if image.name == 'Revelator v'+str(version):
            image_id = image.id;
            break;

    #Create the droplet
    s.Droplet.create(name='rev.'+name,region='ams2',size=size,image=image_id,private_networking=True,ssh_keys=s.Key.all());

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
