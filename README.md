# Yii2 BigQuery Component

This is a Yii2 extension that provides an interface to Google's BigQuery API. It allows you to run queries and retrieve data from BigQuery using Yii2's ActiveRecord syntax.

## Configuration

To use this extension, you need to configure it in your application configuration like this:

```php
'components' => [
    'bigQuery' => [
        'class'   => 'app\components\BigQuery',
        'keyPath' => 'keyfile.json'
    ],
],
```

## Usage

Here’s an example of how to use this extension:

```php
  //To select from database
  $dataset = "your_dataset";
  $query   = (new yii\db\Query)->select(['id', 'username])->from(["{$dataset}.users"]);
  $result  = Yii::$app->bigQuery->asArray($query->createCommand()->getRawSql());
```
