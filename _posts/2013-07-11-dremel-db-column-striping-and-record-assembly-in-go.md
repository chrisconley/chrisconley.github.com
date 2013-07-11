---
layout: post
title: "Dremel DB Column Striping and Record Assembly in Go"
description: ""
category: 
tags: []
---
{% include JB/setup %}


### Dremel DB and Golang Sitting in a Tree
[Dremel DB](http://net.pku.edu.cn/~course/cs501/2012/reading/2010-VLDB-Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf)
is a column store database from Google that allows efficient storage of arbritrarily nested data structures. It's used
to power Google's [BigQuery](https://developers.google.com/bigquery/) as well as a few other commercial databases such
as [CitusDB](http://www.citusdata.com/docs/sql-on-hadoop). The system "scales to thousands of CPUS and petabytes of
data", but we're going to focus on the algorithms that transform the nested data structures from record form to columnar
format and back again.

The Dremel paper does a pretty good job of explaining the algorithms used for doing this, so I thought it'd be fun to
learn [Go](http://golang.org/) by trying to replicate the algos. Lot's of source code is listed below, but you can find full repo at http://github.com/chrisconley/godremel.

We'll go over transforming records to columns (column striping) first, then move on to record assembly.

### Column Striping

We're going to use the example from the Dremel paper:

![Dremel Paper Schema and Columns](/assets/images/schema-columns.png)

The boxes on the left represent the records we're working with, the schema is in the middle, and the column
representations of each field are on the right.

#### Encoding data for columns

Even though we have nested records, we're able to store each column separately without holding an explicit reference to
parent and children values. This is done by storing a repetition level and definition level for each value.

* **Repetition Level:** The level at which the field is repeating at. For example, in
`Links.Forward`, value `20` has not repeated yet, so its repetition level is `0`. The value `40` has a repetition level
of `1` because the level at which it is repeating is `Links` (level `1`).
* **DefinitionLevel Level:** The number of *non-required* fields that are not null from the current value to the topmost
parent value. For example, in `Name.Language.Country`, value `us` has a definition level of 3 because `Name`, `Language`
and `Country` are all `optional` fields and they all are defined. On the other hand, the entry right below `us` is
`null` with a definition level of `2` because only `Name` and `Language` are defined.

If that wasn't confusing enough, let's jump right in and take a look at the records and schema in json format:

#### Records
{% highlight javascript %}
// record1
{
  "id": 10,
  "links" : {
    "forward": [20, 40, 60]
  },
  "names": [{
      "languages": [
        {"code": "en-us", "country": "us"},
        {"code": "en"}
      ],
      "url": "http://A"},
      {"url": "http://B"},
      {"languages": [
        {"code": "en-gb", "country": "gb"}
      ]
    }
  ]
}

// record2
{
  "id": 20,
  "links" : {
    "backward": [10, 30],
    "forward": [80]
  },
  "names": [{
      "url": "http://C"}
  ]
}
{% endhighlight %}

#### Schema
{% highlight javascript %}
{
  "Fields": [
    {"Name": "id", "Kind": "int", "Mode": "required"},
    {"Name": "links", "Kind": "record", "Mode": "optional", "Fields": [
      {"Name": "backward", "Kind": "string", "Mode": "repeated"},
      {"Name": "forward", "Kind": "string", "Mode": "repeated"}
    ]},
    {"Name": "names", "Kind": "record", "Mode": "repeated", "Fields": [
      {"Name": "languages", "Kind": "record", "Mode": "repeated", "Fields": [
        {"Name": "code", "Kind": "string", "Mode": "required"},
        {"Name": "country", "Kind": "string", "Mode": "optional"}
      ]},
      {"Name": "url", "Kind": "string", "Mode": "optional"}
    ]}
  ]
}
{% endhighlight %}

#### StripeRecord function

Our `StripeRecord` function will take our schema, which is just a `Field` and one record. It also takes a `Writer` which
is responsible for determining the field's repetition and definition levels. Some of the supporting functions are listed
below. The function is closely modeled after the column-striping algorithm in Appendix A of the [Dremel
Paper](http://net.pku.edu.cn/~course/cs501/2012/reading/2010-VLDB-Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf).

{% highlight go %}

func StripeRecord(field Field, record interface{}, datastore DataStore, writer Writer, rLevel int) {
  seenFields := map[string]bool{}
  decoder := Decoder{field, record}
  
  // Using a go channel, act like a Python generator and yield fields and values one at a time
  // even if the field is "repeated" and has a list of values.
  for fieldValue := range decoder.ReadValues() {
    childWriter := Writer{fieldValue.Field.Name, fieldValue.Field, fieldValue.Value, &writer}
    childRepetitionLevel := rLevel

    // if we've seen this field already, set childRepetitionLevel to that of the childWriter
    if _, present := seenFields[fieldValue.Field.Name]; present {
       childRepetitionLevel = childWriter.RepeatedFieldDepth()
    } else { // otherwise, just record that we've seen this field 
      seenFields[fieldValue.Field.Name] = true
    }

    // If this value is another "record", call StripeRecord recursively
    if fieldValue.Field.Kind == "record" {
      StripeRecord(fieldValue.Field, fieldValue.Value, datastore, childWriter, childRepetitionLevel)
    } else { // otherwise, we can write a row out to our datastore
      row := Row{childWriter.Value, childRepetitionLevel, childWriter.DefinitionLevel()}
      datastore.WriteRow(childWriter.Path(), row)
    }
  }
}
{% endhighlight %}

#### Supporting Functions

{% highlight go %}
type Field struct {
    Name string
    Kind string // int, string, record
    Mode string // optional, repeated
    Fields []Field
}

type Writer struct {
  Name string
  Field Field
  Value interface{}
  Parent *Writer
}

func (writer *Writer) RepeatedFieldDepth() int {
  depth := 0
  if writer.Field.Mode == "repeated" {
    depth++
  }
  parent := writer.Parent
  for parent.Name != RootWriter.Name {
    if parent.Field.Mode == "repeated" {
      depth++
    }
    parent = parent.Parent
  }
  return depth
}

func (writer *Writer) DefinitionLevel() int {
  depth := 0
  if writer.Field.Mode != "required" && writer.Value != nil {
    depth++
  }
  parent := writer.Parent
  for parent.Name != RootWriter.Name {
    if parent.Field.Mode != "required" && parent.Value != nil {
      depth++
    }
    parent = parent.Parent
  }
  return depth
}


type Decoder struct {
  Field Field
  Record interface{}
}

func (decoder *Decoder) ReadValues() chan FieldValue  {
  c := make(chan FieldValue)
  go func() {
    for _, f := range decoder.Field.Fields {
      recordValue := decoder.getValue(f.Name)
      if f.Mode == "repeated" && recordValue != nil {
        for _, value := range recordValue.([]interface{}) {
          c <- FieldValue{f, value}
        }
      } else {
        c <- FieldValue{f, recordValue}
      }
    }
    close(c)
  }()
  return c
}
{% endhighlight %}

#### And The Test to Set Everything Up

{% highlight go %}
func TestStripeRecord(t *testing.T) {
  var schema Field
  readJson(t, "./schema.json", &schema)

  memstore := MemStore{map[string][]Row{}}

  var record interface{}
  readJson(t, "./record1.json", &record)

  var record2 interface{}
  readJson(t, "./record2.json", &record2)

  StripeRecord(schema, record, &memstore, RootWriter, 0)
  StripeRecord(schema, record2, &memstore, RootWriter, 0)

  countryRows := []Row{
    Row{"us", 0, 3},
    Row{nil, 2, 2},
    Row{nil, 1, 1},
    Row{"gb", 1, 3},
    Row{nil, 0, 1},
  }
  for i := 0; i < len(memstore.Data["names.languages.country"]); i++ {
    if memstore.Data["names.languages.country"][i] != countryRows[i] {
      t.Errorf("Mismatched Rows")
    }
  }
}
{% endhighlight %}

#### Drumroll Please...
{% highlight sh %}
Chriss-MacBook-Pro:go_dremel chrisconley$ go test go_dremel
ok  	go_dremel	0.018s
{% endhighlight %}



### Record Assembly

Almost there...

Check 



