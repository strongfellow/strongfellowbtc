
import nose.tools

import strongfellowbtc.hash as hash

def test_switch_endian():
    little_endian_hex = '9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50'
    big_endian_hex = '503d8319a48348cdc610a582f7bf754b5833df65038606eb48510790dfc99595'


    nose.tools.eq_(big_endian_hex, hash.switch_endian(hash.switch_endian(big_endian_hex)))
    nose.tools.eq_(little_endian_hex, hash.switch_endian(hash.switch_endian(little_endian_hex)))
    nose.tools.eq_(big_endian_hex, hash.switch_endian(little_endian_hex))
    nose.tools.eq_(little_endian_hex, hash.switch_endian(big_endian_hex))
    

def test():
    h = hash.double_sha256('hello')

    little_endian_hex = '9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50'
    big_endian_hex = '503d8319a48348cdc610a582f7bf754b5833df65038606eb48510790dfc99595'


    nose.tools.eq_(little_endian_hex, hash.little_endian_hex(h))
    nose.tools.eq_(big_endian_hex, hash.big_endian_hex(h))
