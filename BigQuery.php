<?php

namespace app\components;

use Yii;
use yii\base\Component;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Numeric;

class BigQuery extends Component
{
    public $keyPath;

    protected $_bigquery;

    public function client()
    {
        if (isset($this->_bigquery))
            return $this->_bigquery;

        Yii::setAlias('@webroot', realpath(dirname(__FILE__).'/../'));
        $path    = $this->keyPath ? Yii::getAlias("@webroot/") . $this->keyPath : Yii::getAlias("@webroot/") . 'web/keyfile.json';
        $keyFile = $this->keyPath ? json_decode(file_get_contents($path), true) : json_decode(file_get_contents($path), true);

        // Authenticating with keyfile data.
        return $this->_bigquery = new BigQueryClient([
            'keyFile' => $keyFile
        ]);
    }

    public function insertRows($rows, $table)
	{
        // $rows = [
        //     [
        //         'insertId' => '1',
        //         'data' => [
        //             'city' => 'Detroit',
        //             'state' => 'MI'
        //         ]
        //     ],
        //     [
        //         'insertId' => '2',
        //         'data' => [
        //             'city' => 'New York',
        //             'state' => 'NY'
        //         ]
        //     ]
        // ];

		$success  = true;
		$bigQuery = self::client();
		$dataset  = $bigQuery->dataset(Yii::$app->params['BigQuery']['dataset']);
		$table    = $dataset->table($table);
		$insertResponse = $table->insertRows($rows);

		if (!$insertResponse->isSuccessful()) {
			$success = false;
            $msg     = "";
			foreach ($insertResponse->failedRows() as $row) {
				foreach ($row['errors'] as $error) {
					$msg .= $error['reason'] . ': ' . $error['message'] . "\n";
				}
			}

            throw new \Exception($msg, 1);
		}

		return $success;
	}

    public function insertRow($row, $table)
	{
        // $row = [
        //     'city' => 'Detroit',
        //     'state' => 'MI'
        // ];

		$success  = true;
		$bigQuery = self::client();
		$dataset  = $bigQuery->dataset(Yii::$app->params['BigQuery']['dataset']);
		$table    = $dataset->table($table);
		$insertResponse = $table->insertRow($row);

		if (!$insertResponse->isSuccessful()) {
			$success = false;
            $msg     = "";

            $row = $insertResponse->failedRows()[0];

            foreach ($row['errors'] as $error) {
                $msg .= $error['reason'] . ': ' . $error['message'] . "\n";
			}

            throw new \Exception($msg, 1);
		}

		return $success;
	}

    public function mergeToMain($rows, $table)
    {
        $bigQuery = self::client();
		$table_temp = "{$table}_temp";
		$dataset_id = Yii::$app->params['BigQuery']['dataset'];
        $main_row   = "";
        $temp_row   = "";

        foreach ($rows as $value) {
            $main_row .= "{$value},";
            $temp_row .= "st.{$value},";
        }

        $main_row = substr($main_row, 0, strlen($main_row) - 1);
        $temp_row = substr($temp_row, 0, strlen($temp_row) - 1);

		$sql = "
			MERGE {$dataset_id}.{$table} pt
			USING {$dataset_id}.{$table_temp} st
			ON pt.id = st.id
			WHEN NOT MATCHED THEN
			INSERT ({$main_row}) VALUES
			({$temp_row})
		";

		try {
			$queryJobConfig = $bigQuery->query($sql);
			$queryResults   = $bigQuery->runQuery($queryJobConfig);

			if ($queryResults->isComplete())
				echo date("Y-m-d H:i:s") .  "Merge Success!\n";
		} catch (\Throwable $th) {
			echo "\n\n SQL >>>> \n {$sql}\n";
			echo date("Y-m-d H:i:s") .  $th;
		}
    }

    public function updateRow($sql)
	{
		$bigQuery = self::client();

		try {
			$queryJobConfig = $bigQuery->query($sql);
			$queryResults   = $bigQuery->runQuery($queryJobConfig);

			if ($queryResults->isComplete())
				return true;
		} catch (\Throwable $th) {
            echo $th;
			return false;
		}
	}

    public function selectRows($sql)
	{
		$bigQuery       = self::client();
		$queryJobConfig = $bigQuery->query($sql);

		try {
			$queryJobConfig->useQueryCache(false);
			$queryResults = $bigQuery->runQuery($queryJobConfig);

			return $queryResults;
		} catch (\Throwable $th){
            Yii::Warning(date("Y-m-d H:i:s") .  $th);
			echo date("Y-m-d H:i:s") .  $th;
		}
	}

	public function asArray($sql)
    {
        $rows = self::selectRows($sql);
        $res  = [];
        
        foreach ($rows as $row) {
            $body = [];
            foreach ($row as $key => $column) {
                if (gettype($column) != 'object')
                    $body[$key] = $column;
                elseif ($column instanceof Numeric)
                    $body[$key] = $column->get();
                elseif ($column instanceof \DateTime) {
                    $date = new \DateTime($column->format('Y-m-d H:i:s'), new \DateTimeZone(Yii::$app->params['timeZone']['saveTimeZone']));
                    $date->setTimezone(new \DateTimeZone(Yii::$app->getTimeZone()));
                    $body[$key] = $date->format("Y-m-d H:i:s");
                }
            }

            $res[] =$body;
        }

        return $res;
    }

    public function count($org_sql)
    {
        $sql   = "SELECT COUNT(*) as row_count FROM ($org_sql)";
        $rows  = self::selectRows($sql);
        $count = 0;

        foreach ($rows as $row) {
            $count = $row['row_count'];
        }

        return $count;
    }
}
