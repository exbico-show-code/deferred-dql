<?php declare(strict_types=1);
/**
 * This file is a part of the Project 3.0 verification system.
 *
 * @author Maksim Fedorov
 * @date   17.12.2019 10:43
 */

namespace Bank30\Tests\functional\Service\Background\DeferredDql;

use Bank30\Tests\_support\FunctionalTester;
use Bank30\Tests\functional\DependenciesTrait;
use Bank30\VerificationBundle\Entity\Document\PassportFields;
use Bank30\VerificationBundle\Entity\ExternalId;
use Bank30\VerificationBundle\Entity\Instance;
use Bank30\VerificationBundle\Entity\JobParams;
use Bank30\VerificationBundle\Entity\Meta\MetadataDocumentScansBag;
use Bank30\VerificationBundle\Entity\Passport;
use Bank30\Service\Background\DeferredDql\CachedDeferredDqlExecutor;
use Bank30\VerificationBundle\Service\Background\Handler\BackgroundDqlCacheHandler;
use Bank30\VerificationBundle\Service\Cache\LazyRedis;
use Bank30\VerificationBundle\Service\Cache\LazyRedisCache;
use Bank30\VerificationBundle\Service\Document\Dto\NullProcessingFlags;
use Codeception\Example;
use Doctrine\Common\Cache\Cache;

/**
 * @noinspection PhpUnused
 */

class CachedDqlExecuteCest
{
    use DependenciesTrait;

    /** @var Cache */
    protected $cache;

    public function _beforeExtend(FunctionalTester $I): void
    {
        /** @var LazyRedisCache|LazyRedis $cache */
        $this->cache = $I->grabService('b3.cache');
        $this->cache->flushAll();
    }

    /**
     * @dataProvider dataProviderExecute
     *
     * @param FunctionalTester $I
     * @param Example          $data
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function execute(FunctionalTester $I, Example $data): void
    {
        $cacheDqlRangeKey = 'easyadmin_deferred_queries';

        $this->loadDocuments($data['documents']);

        /** @var BackgroundDqlCacheHandler $dqlHandler */
        $dqlHandler = $I->grabService('b3.background.dql_cache_handler');
        $dqlHandler->execute(...array_values($data['dql']));

        /** @var CachedDeferredDqlExecutor $queryExecutor */
        $queryExecutor = $I->grabService(CachedDeferredDqlExecutor::class);
        $queryExecutor->executeCached();

        $countInCache = $this->cache->fetch($data['dql']['targetCacheKey']);
        $I->assertEquals($data['expectedCountToCache'], $countInCache, 'Проверим в кеше наше искомое значение');

        $cachedDql = $this->cache->fetch('provision_' . $data['dql']['targetCacheKey']);
        $I->assertFalse($cachedDql, 'Проверим, что DQL в кеше нет');

        $expectedDqlRange = [0 => $data['dql']['targetCacheKey']];
        $cachedDqlRange   = $this->cache->getRedis()->zRange($cacheDqlRangeKey, 0, -1);
        $I->assertEquals($expectedDqlRange, $cachedDqlRange, 'Рейтинг DQL равен 0');
    }

    protected function dataProviderExecute(): array
    {
        return [
            'Результат найден и закеширован'                                => [
                'dql'                  => [
                    'dql'            => 'SELECT count(entity) FROM Bank30\\VerificationBundle\\Entity\\Document entity WHERE entity.createdAt >= :midnight',
                    'params'         => [
                        [
                            'name'  => 'midnight',
                            'value' => date('Y-m-d'),
                            'type'  => 2,
                        ],
                    ],
                    'hints'          => [
                        'doctrine_paginator.distinct' => false,
                        'doctrine.customOutputWalker' => 'Bank30\\VerificationBundle\\Query\\WithoutDiscriminatorWalker',
                    ],
                    'targetCacheKey' => 'easyadmin_old_2019-12-17_SELECT count(d0_.id) AS sclr_0 FROM documents d0_d41d8cd98f00b204e9800998ecf8427e',
                    'ttl'            => 6400,
                ],
                'documents'            => [
                    Passport::create(
                        new ExternalId(1, '1', new Instance('a', 'b')),
                        1,
                        new JobParams(2, 0),
                        new MetadataDocumentScansBag(new PassportFields()),
                        new NullProcessingFlags()
                    ),
                    Passport::create(
                        new ExternalId(2, '1', new Instance('a', 'a')),
                        2,
                        new JobParams(2, 0),
                        new MetadataDocumentScansBag(new PassportFields()),
                        new NullProcessingFlags()
                    ),
                ],
                'expectedCountToCache' => [[1 => 2]],
            ],
            'Результат: 0, закеширован'                                     => [
                'dql'                  => [
                    'dql'            => 'SELECT count(entity) FROM Bank30\\VerificationBundle\\Entity\\Document entity WHERE entity.createdAt >= :midnight',
                    'params'         => [
                        [
                            'name'  => 'midnight',
                            'value' => date('Y-m-d'),
                            'type'  => 2,
                        ],
                    ],
                    'hints'          => [
                        'doctrine_paginator.distinct' => false,
                        'doctrine.customOutputWalker' => 'Bank30\\VerificationBundle\\Query\\WithoutDiscriminatorWalker',
                    ],
                    'targetCacheKey' => 'easyadmin_old_2019-12-17_SELECT count(d0_.id) AS sclr_0 FROM documents d0_d41d8cd98f00b204e9800998ecf8427e',
                    'ttl'            => 6400,
                ],
                'documents'            => [],
                'expectedCountToCache' => [[1 => 0]],
            ],
            'Результат: null (из-за любого исключения), закеширован' => [
                'dql'                  => [
                    'dql'            => 'SELECT pg_sleep(100)',
                    'params'         => [],
                    'hints'          => [],
                    'targetCacheKey' => 'any_key',
                    'ttl'            => 6400,
                ],
                'documents'            => [],
                'expectedCountToCache' => null,
            ],
        ];
    }

    private function loadDocuments($documents): void
    {
        foreach ($documents as $document) {
            $document->clearHash();
            $this->em->persist($document);
        }
        $this->em->flush();
    }
}
