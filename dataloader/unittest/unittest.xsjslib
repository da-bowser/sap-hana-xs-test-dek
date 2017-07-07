/*global jasmine, describe, beforeOnce, beforeEach, it, xit, expect*/
describe("My First Test Suite using Jasmine", function() {
	
	it('should show an assertion that passes', function() {
		expect(1).toBe(1);
	});

	it("should show an negative assertion", function() {
		expect(true).not.toBe(false);
	});

	it("should throw an expected error", function() {
		expect(function() {
			throw new Error("expected error");
		}).toThrowError("expected error");
	});

	//xit = this test case is excluded
	xit("should show an assertion that fails", function() {
		expect(1).toBe(2);
	});

	
	it("lallala", function() {
		
		
		
	});
	
	
});