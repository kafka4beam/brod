defmodule BrodSampleTest do
  use ExUnit.Case
  doctest BrodSample

  test "greets the world" do
    assert BrodSample.hello() == :world
  end
end
