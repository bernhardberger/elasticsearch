<?php

declare(strict_types=1);

namespace Pluswerk\Elasticsearch\Indexer;

class GenericTableIndexer extends AbstractElasticIndexer
{
    public function process(): void
    {
        $client = $this->config->getClient();
        if ($client === null) {
            return;
        }

        $rows = $this->findAllTableEntries();
        $params = [];
        $iterator = 0;

        $this->output->writeln(sprintf('<info>Indexing %s entities of table %s...</info>', count($rows), $this->tableName));

        foreach ($rows as $row) {
            $iterator++;
            $params['body'][] = [
                'index' => [
                    '_index' => $this->config->getIndexName(),
                    '_id' =>  $this->tableName . ':' . $row['uid']
                ],
            ];

            $documentBody = $this->getDocumentBody($row);

            if (isset($documentBody['url']) && empty($documentBody['url'])) {
                $routeConfig = $this->config->getRouteConfig($this->tableName);

                // replace dynamic parameters/placeholders
                \array_walk_recursive($routeConfig['arguments'], function(&$value, $key) use ($row) {
                    $matches = null;
                    if (\preg_match('/\{(\w+)\}/', $value, $matches) === 1) {
                        $value = $row[$matches[1]];
                    }
                });

                $url = (string)$this->config->getSite()->getRouter()->generateUri(
                    (int)$routeConfig['pageUid'],
                    $routeConfig['arguments']
                );

                $documentBody['url'] = $url;
            }

            $documentBody['id'] = $this->tableName . ':' . $row['uid'];

            $params['body'][] = $documentBody;

            // Every 1000 documents stop and send the bulk request
            if ($iterator % 1000 === 0) {
                $responses = $client->bulk($params);

                // erase the old bulk request
                $params = ['body' => []];

                // unset the bulk response when you are done to save memory
                unset($responses);
            }
        }

        if (!empty($params['body'])) {
            $responses = $client->bulk($params);
        }
    }
}
