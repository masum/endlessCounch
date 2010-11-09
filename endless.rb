# -*- coding: utf-8 -*-
require 'rubygems'
require 'httpclient'
require 'json/lexer'
require 'csv'
require 'nkf'
require 'uri'
# CouchDBクライアント
module CouchDB
  class Client
    def initialize(host, db, proxy=ENV["http_proxy"])
      @host = host
      @db = db
      @client = HTTPClient.new( proxy, "http client." )
    end
    def existDataBase?()
      return get(@db)['error'] == nil
    end
    def createDataBase()
      return put(@db,'')
    end
    def bulk(data)
      return post(@db+"/_bulk_docs",data)
    end
    def delete(uri, value=nil)
      request( :delete, uri, value.to_json)
    end
    def get(uri)
      request( :get, uri)
    end
    def head(uri)
      begin
        request( :head, uri)
        return true
      rescue
        return false if $!.to_s =~/^404/
        raise $!
      end
    end
    def put(uri, value)
      request( :put, uri, value.to_json)
    end
    def post(uri, value)
      request( :post, uri, value.to_json)
    end
    def request(method,uri,body=nil)
      res = @client.request( method, "#{@host}/#{uri}",
        body ? {"content-type"=>"application/json"} : nil, body)
      error( res, method, uri, body ) if res.status >= 400
      JSON::Lexer.new(res.content).nextvalue if res.content rescue nil
    end
  private
    def error( res, method, uri, body )
      message = ""
      if res.content
        parsed = JSON::Lexer.new(res.content).nextvalue
        message = "#{parsed["error"]} : #{parsed["reason"]}" if parsed
      end
      # raise "#{res.status} : #{method} #{uri} #{message} \n#{body}"
    end
  end
end

host = "http://localhost:5984/" # CouchDBサーバー
db = "endless"                  # データベース名
bulkRange = 10000               # １度にバルク登録する件数
valueRange = 10000              # value にセットする値のランダム値
totalCount = 0                  # トータルの登録数

# CouchDBクラスインスタンス生成
couch = CouchDB::Client.new(host,db)

# データベースがなければ新規作成
couch.createDataBase() unless couch.existDataBase?

# バルク登録用変数宣言
bulk = Hash::new
bulk["docs"] = []

# 開始時間
startTime = Time.now

# ループ処理
count = 0
while true 
  totalCount += 1
  count += 1
  bulk['docs'].push({'value'=>rand(valueRange)})
  if count >= bulkRange then
    couch.bulk(bulk)
    hours = (Time.now - startTime).divmod(60*60)
    mins = hours[1].divmod(60)
    puts "#{totalCount} : #{hours[0].to_i}h/#{mins[0].to_i}m/#{mins[1]}s"
    bulk["docs"] = []
    count = 0
  end
end





