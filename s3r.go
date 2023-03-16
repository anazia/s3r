package main

import (
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/dustin/go-humanize"
)

// Delete an oject
func Delete(b string, config *aws.Config, k *string, v *string) error {
    paramsDelete := &s3.DeleteObjectInput{
        Bucket:    aws.String(b),
        Key:       k,
        VersionId: v,
    }

    // Create an EC2 service object
    connect := s3.New(session.New(), config)

    _, err := connect.DeleteObject(paramsDelete)
    if err != nil {
        log.Println(err)
    }

    return nil
}

// User confirmation
func askForConfirmation(bucket string, path string) bool {
    var response string

    log.Printf("Path: \"s3://%s/%s\"", bucket, path)
    log.Print("Are you sure you want to delete all of these objects? [yes/no]: ")

    for {
        _, err := fmt.Scanf("%s", &response)

        if err != nil {
            log.Fatal(err)
        }

        if response == "yes" {
            return true
        }

        if response == "no" {
            log.Println("Didn't think so...")
            return false
        }
        log.Print("Please enter \"yes\" to confirm or \"no\" to cancel: ")
    }
}

func main() {

    log.SetFlags(0)
    log.SetPrefix("s3r | ")
    log.Println("====================================================================")
    log.Println()

    var path string

    var Usage = func() {
        log.Printf("Usage of %s:\n", os.Args[0])
        log.Println()
        log.Println("Please supply at least a bucket name & if needed a path to delete:")
        log.Printf("%s bucket.name path/to/delete\n", os.Args[0])
        log.Println()
        log.Print("Flags:\n\n")
        flag.PrintDefaults()
    }

    flag.Usage = Usage

    noList := flag.Bool("no", false, "do not list the contents first")
    flag.Parse()

    // Check passed arguments
    if len(flag.Args()) < 1 {
        log.Println("Please supply at least a bucket name & if needed a path to delete:")
        log.Fatal("s3r bucket.name path/to/delete")
    } else if len(flag.Args()) == 1 {
        path = ""
    } else if len(flag.Args()) == 2 {
        path = flag.Arg(1)
    } else if len(flag.Args()) > 2 {
        log.Println("Please only enter 2 arguments:")
        log.Fatal("s3r bucket.name path/to/delete")
    }

    bucket := flag.Arg(0)

    // Create an EC2 service object
    sess := session.New()
    connect := s3.New(sess)

    // Get bucket region or set default to "us-east-1"
    log.Printf("Getting bucket region... ")
    getBucketRegion, err := connect.GetBucketLocation(&s3.GetBucketLocationInput{
        Bucket: &bucket,
    })

    if err != nil {
        log.Fatal(err)
    }

    var region string
    if getBucketRegion.LocationConstraint == nil {
        region = "us-east-1"
    } else {
        region = *getBucketRegion.LocationConstraint
    }

    log.Printf("Using region: %s\n", region)
    log.Println()

    config := aws.NewConfig().WithRegion(region)

    // set Variables
    var (
        pagesVersions int
        countVersions int
        countDeleted  int
        countObjects  int
        bytesVersions int64
        bytesObjects  int64
    )

    connect = s3.New(sess, config)

    params := &s3.ListObjectVersionsInput{
        Bucket: aws.String(bucket),
        Prefix: aws.String(path),
    }

    if !*noList {

        log.Print("Counting progress:\n")
        log.Println()

        // Get All Versions and Delete Markers
        err = connect.ListObjectVersionsPages(params,
            func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
                pagesVersions++
                countVersions += len(page.Versions)
                countDeleted += len(page.DeleteMarkers)

                for _, obj := range page.Versions {
                    if *obj.IsLatest {
                        bytesObjects += *obj.Size
                        countObjects++
                    }
                    bytesVersions += *obj.Size
                }

                log.Printf("Page %d: %d (%s) objects so far \n", pagesVersions, countObjects, humanize.Bytes(uint64(bytesObjects)))
                log.Printf("Page %d: %d (%s) versions so far \n", pagesVersions, countVersions, humanize.Bytes(uint64(bytesVersions)))
                log.Printf("Page %d: %d delete markers so far \n", pagesVersions, countDeleted)
                return true
            })

        if err != nil {
            log.Fatal(err)
        }

        // summary
        log.Println()

        if countObjects+countVersions+countDeleted == 0 {
            log.Fatalf("This path \"s3://%s/%s\" is empty!!!\n", bucket, path)
        }

        log.Printf("This path \"s3://%s/%s\" contains:\n", bucket, path)
        log.Println()
        log.Printf("Total size of versions: %s (%d bytes)\n", humanize.Bytes(uint64(bytesVersions)), bytesVersions)
        log.Printf("Total size of objects: %s (%d bytes)\n", humanize.Bytes(uint64(bytesObjects)), bytesObjects)
        log.Println()
        log.Printf("Total objects: %d\n", countObjects)
        log.Printf("Total object versions: %d\n", countVersions)
        log.Printf("Total delete markers: %d\n", countDeleted)
        log.Println()
        log.Println("Total costs for this:")
        log.Printf("- $%f / month\n", float64(bytesVersions)/1000000000.0*0.03)
        log.Printf("- $%f / day\n", float64(bytesVersions)/1000000000.0*0.03/30)
    }

    // Do it...
    c := askForConfirmation(bucket, path)
    if c {
        log.Println("Deleting...")

        pagesVersions := 0
        err = connect.ListObjectVersionsPages(params,
            func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
                pagesVersions++
                countVersions += len(page.Versions)
                countDeleted += len(page.DeleteMarkers)

                log.Println()
                log.Printf("Page %d: Versions:", pagesVersions)
                log.Println()
                for _, ver := range page.Versions {
                    Delete(bucket, config, ver.Key, ver.VersionId)
                    log.Printf("P%d: Deleted: Key: s3://%s/%v | VersionId: %v \n", pagesVersions, bucket, aws.StringValue(ver.Key), aws.StringValue(ver.VersionId))
                }

                log.Println()
                log.Printf("Page %d: Delete Markers:", pagesVersions)
                log.Println()

                for _, del := range page.DeleteMarkers {
                    Delete(bucket, config, del.Key, del.VersionId)
                    log.Printf("P%d: Deleted: Delete Marker: s3://%s/%v | VersionId: %v \n", pagesVersions, bucket, aws.StringValue(del.Key), aws.StringValue(del.VersionId))
                }

                log.Println()
                log.Println("====================================================================")

                return true
            })

        if err != nil {
            log.Fatal(err)
        }
    }
}
