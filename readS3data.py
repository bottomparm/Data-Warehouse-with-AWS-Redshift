from create_clients import s3Client


def readS3Data():
    s3 = s3Client()
    sampleDbBucket = s3.Bucket("udacity-dend")
    # for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    #     print(obj)
    for obj in sampleDbBucket.objects.all():
        print(obj)


def main():
    readS3Data()


if __name__ == "__main__":
    main()
