# `clj-riak`

Clojure bindings to the [Riak](http://www.basho.com/Riak.html) [Protocol Buffers API](https://wiki.basho.com/display/RIAK/PBC+API).

## Installation

`clj-riak` is available as a Maven artifact from [Clojars](http://clojars.org/clj-riak):

    :dependencies
      [[org.clojars.ossareh/clj-riak "0.1.0-SNAPSHOT"] ...]

## Usage

For an introduction to the library, see the blog post [Exploring Riak with Clojure](http://mmcgrana.github.com/2010/08/riak-clojure.html)

## Differences in this fork

Support for links
    
    ;; an example put fn
    (defn put [bucket key data]
      (let [links (or (:links (meta data)) '())
            data {:value (.getBytes (json/json-str data))
                  :content-type "application/json"
                  :links links}]

        (riak/put riak-client
                  bucket
                  key
                  data)))
        
    (put "test-bucket" "test-key"
         (with-meta {:a 1} {:links '({:bucket "foo"
                                      :key "bar"
                                      :tag "foobar"})}))

## TODO

In my personal projects there are a great deal of helper functions to
make storing links / objects / etc significantly shorter than in the
example above. I should try to roll them into this code.

## License

Released under the MIT License: <http://www.opensource.org/licenses/mit-license.php>
