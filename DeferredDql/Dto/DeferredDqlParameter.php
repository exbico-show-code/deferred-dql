<?php declare(strict_types=1);
/**
 * This file is a part of the Project 3.0 verification system.
 *
 * @author Maksim Fedorov
 * @date   17.12.2019 15:44
 */

namespace Bank30\Service\Background\DeferredDql\Dto;

class DeferredDqlParameter
{
    /** @var string */
    private $name;

    /** @var mixed */
    private $value;

    /** @var mixed|null */
    private $type;

    public function __construct(string $name, $value, $type = null)
    {
        $this->name  = $name;
        $this->value = $value;
        $this->type  = $type;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getValue()
    {
        return $this->value;
    }

    public function getType()
    {
        return $this->type;
    }

    public function __toString(): string
    {
        return sprintf('Name: %s, Value: %s', $this->name, $this->value);
    }
}