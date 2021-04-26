<?php

declare(strict_types=1);

namespace Pluswerk\Elasticsearch\Indexer;

class GenericTableIndexer extends AbstractIndexer
{
    public function process(): void
    {
        $client = $this->config->getClient();

        $results = $this->findAllTableEntries();
        $params = [];
        $iterator = 0;

        $this->output->writeln(sprintf('<info>Indexing %s entities of table %s...</info>', count($results), $this->tableName));

        foreach ($results as $result) {
            $iterator++;
            $params['body'][] = [
                'index' => [
                    '_index' => $this->config->getIndexName(),
                    '_id' =>  $this->tableName . ':' . $result['uid']
                ],
            ];

            $documentBody = $this->getDocumentBody($result);

            if (isset($documentBody['url']) && empty($documentBody['url'])) {
                $uriBuilderConfig = $this->config->getUriBuilderConfig($this->index, $this->tableName);
                $uriBuilderConfig['uid'] = $result['uid'];
                $documentBody['url'] = $this->buildUrlFor($uriBuilderConfig);
            }

            $documentBody['id'] = $this->tableName . ':' . $result['uid'];

            $params['body'][] = $documentBody;

            // Every 1000 documents stop and send the bulk request
            if ($iterator % 1000 === 0) {
                $client->bulk($params);

                // erase the old bulk request
                $params = ['body' => []];
            }
        }

        if (!empty($params['body'])) {
            $client->bulk($params);
        }
    }
}
