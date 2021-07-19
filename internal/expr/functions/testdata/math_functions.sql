-- test: math.floor
> math.floor(2.3)
2.0
> math.floor(2)
2
! math.floor('a')
'floor(arg1) expects arg1 to be a number'

-- test: math.abs
> math.abs(NULL)
NULL
> math.abs(-2)
2
> math.abs(-2.0)
2.0
! math.abs(-9223372036854775808)
'integer overflow'
! math.abs('foo')
'abs(arg1) expects arg1 to be a number'

-- test: math.acos
> math.acos(1)
0.0
> math.acos(0.5)
1.0471975511965976
! math.acos(2)
'out of range'
! math.acos(-2)
'out of range'
! math.acos(2.2)
'out of range'
! math.acos(-2.2)
'out of range'
! math.acos('foobar')
'acos(arg1) expects arg1 to be a number'
