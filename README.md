# S3 Selector

When selecting large amounts of data via the AWS SDK, it collects all results into memory and then provides you an enumerable. This is bad for very large result sets. This gem solves that by streaming results in, instead.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 's3_selector'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install s3_selector

## Usage

### With S3 Files

```ruby
bucket = Aws::S3::Bucket.new("my-bucket")
s3_files = bucket.objects(prefix: "folder-with-files")

results = S3Selector::ResultsStream.new(s3_files: []).records
results.each {|r| puts r}
```
