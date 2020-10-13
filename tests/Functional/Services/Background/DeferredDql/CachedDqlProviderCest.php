<?php declare(strict_types=1);
/**
 * This file is a part of the Project 3.0 verification system.
 *
 * @author Maksim Fedorov
 * @date   17.12.2019 10:43
 */

namespace Bank30\Tests\functional\Service\Background\DeferredDql;

use Bank30\Service\Background\DeferredDql\CachedDeferredDqlProvider;
use Bank30\Service\Background\DeferredDql\Dto;
use Bank30\Tests\_support\FunctionalTester;
use Codeception\Example;

/**
 * @noinspection PhpUnused
 */

class CachedDqlProviderCest
{
    /** @var \Redis */
    private $redis;

    public function _before(FunctionalTester $I): void
    {
        $this->redis = $I->grabService('b3.cache')->getRedis();
        $this->redis->flushAll();
    }

    /**
     * @dataProvider dataProviderNegativeCached
     *
     * @param FunctionalTester $I
     * @param Example          $data
     */
    public function handleNegativeCachedRange(FunctionalTester $I, Example $data): void
    {
        $rangeKey = 'easyadmin_deferred_queries';
        $dqlKey   = $data['dql']->getDeferredDql()->getTargetCacheKey();

        $this->redis->zIncrBy($rangeKey, $data['startCachedScore'], $dqlKey);
        /** @var CachedDeferredDqlProvider $cachedProvider */
        $cachedProvider = $I->grabService(CachedDeferredDqlProvider::class);

        $cachedRange = $this->redis->zRevRange($rangeKey, 0, -1, true);
        $I->assertEquals($data['startCachedScore'], $cachedRange[$dqlKey], 'Проверим, что score указан.');

        $cachedProvider->deleteCachedWithDowngrade($data['dql']);

        $cachedRange = $this->redis->zRevRange($rangeKey, 0, -1, true);
        $I->assertEquals($data['expectedRangeValue'], $cachedRange[$dqlKey], 'Проверим корректное уменьшение рейтинга.');
    }

    protected function dataProviderNegativeCached(): \Generator
    {
        $toDecrementScore = 50;
        // Убавляем на значение больше закешированного
        yield ['dql' => $this->createCachedDqlWithScore($toDecrementScore), 'startCachedScore' => 10, 'expectedRangeValue' => 0,];

        // Убавляем на значение равное закешированному
        yield ['dql' => $this->createCachedDqlWithScore($toDecrementScore), 'startCachedScore' => 50, 'expectedRangeValue' => 0,];

        // Убавляем на значение меньше закешированного
        yield ['dql' => $this->createCachedDqlWithScore($toDecrementScore), 'startCachedScore' => 70, 'expectedRangeValue' => 20,];
    }

    private function createCachedDqlWithScore(int $score): Dto\CachedDeferredDql
    {
        return new Dto\CachedDeferredDql(
            Dto\DeferredDql::create(
                '__SELECT__',
                [
                    ['name' => '__NAME__', 'value' => '__VALUE__'],
                ],
                ['hint' => '__VALUE__'],
                '__CACHE_KEY__',
                200
            ), $score
        );
    }
}
